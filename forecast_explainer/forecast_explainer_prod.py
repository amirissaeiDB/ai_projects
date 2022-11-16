# Databricks notebook source
# MAGIC %md
# MAGIC # Forecast Explainer
# MAGIC This notebook is designed to generate the underlying data used by the forecast explainer dashboard [see here](https://us-west-2b.online.tableau.com/#/site/databrickstableaucom/views/ForecastExplainer1_0V/Highleveldashboard-peraccount).
# MAGIC 
# MAGIC 0. First build a master table that includes current and next quarter forecasts
# MAGIC 0. Train Xgboost models on subset of data (pre-defined features)
# MAGIC 0. Use Shap to explain the forecast
# MAGIC 0. Normalize the values (remove the bias) for reporting
# MAGIC 0. Report the direction

# COMMAND ----------

# MAGIC %sql -- line up the data the data from the account-level forecasts generated weekly by the forecasting team
# MAGIC -- we rewrite the table every week with the latest forecasts from the forecasting pipeline
# MAGIC create
# MAGIC or replace table usage_forecasting_prod.fe_dashboard as (
# MAGIC   with forecast as (
# MAGIC     select
# MAGIC       sfdcAccountId,
# MAGIC       date,
# MAGIC       runDate as train_date,
# MAGIC       forecast,
# MAGIC       case
# MAGIC         when extract(
# MAGIC           month
# MAGIC           from
# MAGIC             runDate
# MAGIC         ) in (2, 3, 4) then 1
# MAGIC         when extract(
# MAGIC           month
# MAGIC           from
# MAGIC             runDate
# MAGIC         ) in (5, 6, 7) then 2
# MAGIC         when extract(
# MAGIC           month
# MAGIC           from
# MAGIC             runDate
# MAGIC         ) in (8, 9, 10) then 3
# MAGIC         when extract(
# MAGIC           month
# MAGIC           from
# MAGIC             runDate
# MAGIC         ) in (11, 12, 1) then 4
# MAGIC       end as train_quarter,
# MAGIC       case
# MAGIC         when extract(
# MAGIC           month
# MAGIC           from
# MAGIC             date
# MAGIC         ) in (2, 3, 4) then 1
# MAGIC         when extract(
# MAGIC           month
# MAGIC           from
# MAGIC             date
# MAGIC         ) in (5, 6, 7) then 2
# MAGIC         when extract(
# MAGIC           month
# MAGIC           from
# MAGIC             date
# MAGIC         ) in (8, 9, 10) then 3
# MAGIC         when extract(
# MAGIC           month
# MAGIC           from
# MAGIC             date
# MAGIC         ) in (11, 12, 1) then 4
# MAGIC       end as date_quarter,
# MAGIC       case
# MAGIC         when extract(
# MAGIC           month
# MAGIC           from
# MAGIC             runDate
# MAGIC         ) = 1 then extract(
# MAGIC           year
# MAGIC           from
# MAGIC             runDate
# MAGIC         )
# MAGIC         else extract(
# MAGIC           year
# MAGIC           from
# MAGIC             runDate
# MAGIC         ) + 1
# MAGIC       end as train_year,
# MAGIC       case
# MAGIC         when extract(
# MAGIC           month
# MAGIC           from
# MAGIC             date
# MAGIC         ) = 1 then extract(
# MAGIC           year
# MAGIC           from
# MAGIC             date
# MAGIC         )
# MAGIC         else extract(
# MAGIC           year
# MAGIC           from
# MAGIC             date
# MAGIC         ) + 1
# MAGIC       end as date_year
# MAGIC     from
# MAGIC       usage_forecasting_acct.account_date_forecast_history
# MAGIC   ),
# MAGIC   forecast_current_quarter as (
# MAGIC     select
# MAGIC       sfdcAccountId,
# MAGIC       train_date,
# MAGIC       count(*) / 7 current_quarter_num_week,
# MAGIC       sum(forecast) as current_q_forecast
# MAGIC     from
# MAGIC       forecast
# MAGIC     where
# MAGIC       train_quarter = date_quarter
# MAGIC       and train_year = date_year
# MAGIC     group by
# MAGIC       sfdcAccountId,
# MAGIC       train_date
# MAGIC   ),
# MAGIC   forecast_next_quarter as (
# MAGIC     select
# MAGIC       sfdcAccountId,
# MAGIC       train_date,
# MAGIC       count(*) / 7 next_quarter_num_week,
# MAGIC       sum(forecast) as next_q_forecast
# MAGIC     from
# MAGIC       forecast
# MAGIC     where
# MAGIC       date_quarter = case
# MAGIC         when mod(train_quarter + 1, 5) = 0 then 1
# MAGIC         else mod(train_quarter + 1, 5)
# MAGIC       end
# MAGIC       and date_year = case
# MAGIC         when mod(train_quarter + 1, 5) = 0 then train_year + 1
# MAGIC         else train_year
# MAGIC       end
# MAGIC     group by
# MAGIC       sfdcAccountId,
# MAGIC       train_date
# MAGIC   )
# MAGIC   select
# MAGIC     a.*,
# MAGIC     coalesce(b.next_q_forecast, 0) as next_q_forecast,
# MAGIC     coalesce(next_quarter_num_week, 1) as next_quarter_num_week
# MAGIC   from
# MAGIC     forecast_current_quarter as a
# MAGIC     left join forecast_next_quarter as b on a.sfdcAccountId = b.sfdcAccountId
# MAGIC     and a.train_date = b.train_date
# MAGIC )

# COMMAND ----------

# MAGIC %sql -- create the final dtaaset with pre-defined list of features (product, consumption and account features)
# MAGIC create
# MAGIC or replace view data_for_shap_prod as (
# MAGIC   with current_quarter_average as (
# MAGIC     select
# MAGIC       sfdcAccountId,
# MAGIC       train_Date,
# MAGIC       current_q_forecast as current_quarter_forecast,
# MAGIC       current_q_forecast / current_quarter_num_week as average_current_quarter_weekly_average
# MAGIC     from
# MAGIC       usage_forecasting_prod.fe_dashboard
# MAGIC   ),
# MAGIC   next_quarter_average as (
# MAGIC     select
# MAGIC       sfdcAccountId,
# MAGIC       train_Date,
# MAGIC       next_q_forecast as next_quarter_forecast,
# MAGIC       next_q_forecast / next_quarter_num_week as average_next_quarter_weekly_average
# MAGIC     from
# MAGIC       usage_forecasting_prod.fe_dashboard
# MAGIC   )
# MAGIC   select
# MAGIC     a.*,
# MAGIC     b.next_quarter_forecast,
# MAGIC     b.average_next_quarter_weekly_average,
# MAGIC     c.noUsageSince12WeeksAgo,
# MAGIC     c.real,
# MAGIC     c.annual_dbu_commit_imputed,
# MAGIC     c.employee_count_imputed,
# MAGIC     c.annual_revenue_imputed,
# MAGIC     c.ae_presence_imputed,
# MAGIC     c.sa_presence_imputed,
# MAGIC     c.cse_presence_imputed,
# MAGIC     c.rsa_presence_imputed,
# MAGIC     c.ae_tenure_months_imputed,
# MAGIC     c.sa_tenure_months_imputed,
# MAGIC     c.cse_tenure_months_imputed,
# MAGIC     c.rsa_tenure_months_imputed,
# MAGIC     c.ae_curracct_oppdollar_closed_this_day_imputed,
# MAGIC     c.ae_anyacct_oppdollar_closed_this_day_imputed,
# MAGIC     c.ae_count_accounts_imputed,
# MAGIC     c.contract_committed_amount_imputed,
# MAGIC     c.contract_expected_burndown_imputed,
# MAGIC     c.contract_actual_and_forecasted_burndown_imputed,
# MAGIC     c.ae_curracct_oppdollar_closed_todate_imputed,
# MAGIC     c.ae_anyacct_oppdollar_closed_todate_imputed,
# MAGIC     c.automated_dbus,
# MAGIC     c.interactive_dbus,
# MAGIC     c.sql_dbus,
# MAGIC     c.AutoML_dollars,
# MAGIC     c.AutomatedJobRun_dollars,
# MAGIC     c.BITool_dollars,
# MAGIC     c.CommandRun_dollars,
# MAGIC     c.Databricks_numUsers,
# MAGIC     c.DatabricksAWS_dollars,
# MAGIC     c.DatabricksAzure_dollars,
# MAGIC     c.DatabricksGCP_dollars,
# MAGIC     c.DatabricksSQL_dollars,
# MAGIC     c.Delta_dollars,
# MAGIC     c.FeatureStore_dollars,
# MAGIC     c.Fivetran_dollars,
# MAGIC     c.GPU_dollars,
# MAGIC     c.InteractiveJobRun_dollars,
# MAGIC     c.JobRun_dollars,
# MAGIC     c.ML_dollars,
# MAGIC     c.MLFlow_dollars,
# MAGIC     c.MLR_dollars,
# MAGIC     c.MultitaskingJob_dollars,
# MAGIC     c.NotebookWorkflowCommandRun_dollars,
# MAGIC     c.Photon_dollars,
# MAGIC     c.PowerBI_dollars,
# MAGIC     c.PyTorch_dollars,
# MAGIC     c.PythonCommandRun_dollars,
# MAGIC     c.RCommandRun_dollars,
# MAGIC     c.SQLCommandRun_dollars,
# MAGIC     c.ScalaCommandRun_dollars,
# MAGIC     c.Sklearn_dollars,
# MAGIC     c.Snowflake_dollars,
# MAGIC     c.SparkML_dollars,
# MAGIC     c.StructuredStreaming_dollars,
# MAGIC     c.Synapse_dollars,
# MAGIC     c.Tableau_dollars,
# MAGIC     c.TensorFlow_dollars,
# MAGIC     c.dollars_lag1,
# MAGIC     c.dollars_lag2,
# MAGIC     c.dollars_lag3,
# MAGIC     c.dollars_lag4
# MAGIC   from
# MAGIC     current_quarter_average as a
# MAGIC     inner join next_quarter_average as b on a.sfdcAccountId = b.sfdcAccountId
# MAGIC     and a.train_Date = b.train_Date
# MAGIC     inner join usage_forecasting_acct.account_week_master as c on a.sfdcAccountId = c.sfdcAccountId
# MAGIC     and a.train_Date = c.weekStart + 7
# MAGIC )

# COMMAND ----------

# train different models per quarter, per forecast_as_Of date 
import xgboost as xgb
import shap
import pandas as pd
from sklearn.model_selection import train_test_split

def run(df):
  '''this funciton iterates through multiple vintages, train the data for current and next quarters and write the shapley values to the Delta tables'''
  
  for date in list(date_to_consider.train_date.unique()):
    print(date)
    data = df[df['train_Date'] == date]
    X = data.iloc[:,8:]
    count = 1
    for q in list(quarter[quarter['train_date']== date].iloc[0,1:]):
      print(q)
      if count == 1:
        y = data["average_current_quarter_weekly_average"]
        print("average_current_quarter_weekly_average")
      else:
        y = data["average_next_quarter_weekly_average"]
        print("average_next_quarter_weekly_average")
      count = count + 1
      model_xgb.fit(X, y)
      pred = model_xgb.predict(X)
      shap.initjs()
      explainer = shap.TreeExplainer(model_xgb)
      shap_values = explainer.shap_values(X, check_additivity=False)
      k = pd.DataFrame(shap_values)
      k.columns = X.columns
      df_out = pd.concat([data.reset_index(), k.add_suffix('_shap')], axis = 1).iloc[:,1:]
      df_out['bv'] = explainer.expected_value
      df_out['quarter_explained'] = q
      spark_df = spark.createDataFrame(df_out)
      spark_df.write.format("delta").mode("append").saveAsTable("usage_forecasting_prod.forecast_explainer_salesforce")

# real customers are treated differently than non-real customers      
df_real = spark.sql("select * from data_for_shap_prod where !(real = 0 or noUsageSince12WeeksAgo)").toPandas()
df_real = df_real.fillna(0)

df_small = spark.sql("select * from data_for_shap_prod where (real = 0 or noUsageSince12WeeksAgo)").toPandas()
df_small = df_small.fillna(0)

# we make sure to NOT train the model if the shapley values are already ready for those dates
date_to_consider = spark.sql("select distinct train_date from data_for_shap_prod minus select distinct train_date from usage_forecasting_prod.forecast_explainer_salesforce").toPandas()

# setting up financial quarters
quarter = spark.sql("select train_date, case when extract(month from train_date) in (2, 3, 4) then 'q1' when extract( month from train_date) in (5, 6, 7) then 'q2' when extract( month from train_date ) in (8, 9, 10) then 'q3' when extract( month from train_date) in (11, 12, 1) then 'q4' end as c_quarter, case when extract(month from train_date) in (2, 3, 4) then 'q2' when extract( month from train_date) in (5, 6, 7) then 'q3' when extract( month from train_date ) in (8, 9, 10) then 'q4' when extract( month from train_date) in (11, 12, 1) then 'q1' end as n_quarter from (select distinct train_date from data_for_shap_prod)").toPandas()

# train the XGboost model 
model_xgb = xgb.XGBRegressor(colsample_bytree=0.055, 
                             gamma=1.5, 
                             learning_rate=0.02, 
                             max_depth=12, 
                             n_estimators=1000,
                             subsample=0.7, 
                             objective='reg:linear',
                             booster='gbtree',
                             reg_alpha=0.0, 
                             eval_metric = 'rmse', 
                             silent=1, 
                             random_state =7,
                            )
print("real")
run(df_real)
print("small")
run(df_small)

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- create master table for forecast explainer
# MAGIC create
# MAGIC or replace view forecast_explainer_master as (
# MAGIC   select
# MAGIC     fe.*,
# MAGIC     ft.accountName,
# MAGIC     ft.bu,
# MAGIC     ft.sub_bu
# MAGIC   from
# MAGIC     usage_forecasting_prod.forecast_explainer_salesforce as fe
# MAGIC     left join (
# MAGIC       select
# MAGIC         distinct accountID,
# MAGIC         accountName,
# MAGIC         bu,
# MAGIC         sub_bu
# MAGIC       from
# MAGIC         finance.dbu_dollars
# MAGIC       union
# MAGIC       select
# MAGIC         distinct accountID,
# MAGIC         accountName,
# MAGIC         bu,
# MAGIC         sub_bu
# MAGIC       from
# MAGIC         finance.pvc_usage
# MAGIC       union
# MAGIC       select
# MAGIC         distinct accountID,
# MAGIC         accountName,
# MAGIC         bu,
# MAGIC         sub_bu
# MAGIC       from
# MAGIC         finance.pubsec_usage
# MAGIC     ) as ft on fe.sfdcAccountId = ft.accountID
# MAGIC );

# COMMAND ----------

# normalize the shapely values and calculate the relative strength for all 3 dimensions of the underlying data (consumption, product and account)

df = spark.sql("select * from forecast_explainer_master").toPandas().set_index(["sfdcAccountId","train_Date","bu","sub_bu","accountName","quarter_explained"])
columns_account = list(set(spark.sql("select * from forecast_explainer_master").columns).intersection(set(spark.sql("select old_name from ai.column_names where type='account' and shap_ind = 1").toPandas()["old_name"])))
columns_product = list(set(spark.sql("select * from forecast_explainer_master").columns).intersection(set(spark.sql("select old_name from ai.column_names where type='product' and shap_ind = 1").toPandas()["old_name"])))
columns_consumption = list(set(spark.sql("select * from forecast_explainer_master").columns).intersection(set(spark.sql("select old_name from ai.column_names where type='consumption' and shap_ind = 1").toPandas()["old_name"])))

account_df = df[columns_account]
product_df = df[columns_product]
consumption_df = df[columns_consumption]

account_df["abs_total_account"] = abs(account_df.iloc[:, :]).sum(axis = 1)
product_df["abs_total_product"] = abs(product_df.iloc[:, :]).sum(axis = 1)
consumption_df["abs_total_consumption"] = abs(consumption_df.iloc[:, :]).sum(axis = 1)

account_df = abs(account_df.iloc[:, :]).div(account_df["abs_total_account"].values, axis=0)
product_df = abs(product_df.iloc[:, :]).div(product_df["abs_total_product"].values, axis=0)
consumption_df = abs(consumption_df.iloc[:, :]).div(consumption_df["abs_total_consumption"].values, axis=0)

account_df.reset_index(inplace=True)
product_df.reset_index(inplace=True)
consumption_df.reset_index(inplace=True)

spark.createDataFrame(account_df).write.mode("overwrite").format("delta").saveAsTable("ai.account_shapley")
spark.createDataFrame(product_df).write.mode("overwrite").format("delta").saveAsTable("ai.product_shapley")
spark.createDataFrame(consumption_df).write.mode("overwrite").format("delta").saveAsTable("ai.consumption_shapley")

# COMMAND ----------

# create diretions for each of the dimensions
df = spark.sql("select * from forecast_explainer_master").toPandas().set_index(["sfdcAccountId","train_Date","bu","sub_bu","accountName","quarter_explained"])
columns_account = list(set(spark.sql("select * from forecast_explainer_master").columns).intersection(set(spark.sql("select old_name from ai.column_names where type='account' and shap_ind = 1").toPandas()["old_name"])))
columns_product = list(set(spark.sql("select * from forecast_explainer_master").columns).intersection(set(spark.sql("select old_name from ai.column_names where type='product' and shap_ind = 1").toPandas()["old_name"])))
columns_consumption = list(set(spark.sql("select * from forecast_explainer_master").columns).intersection(set(spark.sql("select old_name from ai.column_names where type='consumption' and shap_ind = 1").toPandas()["old_name"])))

account_df = df[columns_account]
product_df = df[columns_product]
consumption_df = df[columns_consumption]


account_df[account_df > 0] = 1
account_df[account_df <= 0] = 0

product_df[product_df > 0] = 1
product_df[product_df <= 0] = 0

consumption_df[consumption_df > 0] = 1
consumption_df[consumption_df <= 0] = 0

account_df.reset_index(inplace=True)
product_df.reset_index(inplace=True)
consumption_df.reset_index(inplace=True)


spark.sql("drop table ai.account_shapley_direction")
spark.sql("drop table ai.product_shapley_direction")
spark.sql("drop table ai.consumption_shapley_direction")
spark.createDataFrame(account_df).write.mode("overwrite").format("delta").saveAsTable("ai.account_shapley_direction")
spark.createDataFrame(product_df).write.mode("overwrite").format("delta").saveAsTable("ai.product_shapley_direction")
spark.createDataFrame(consumption_df).write.mode("overwrite").format("delta").saveAsTable("ai.consumption_shapley_direction")

# COMMAND ----------

# do the same (relative strength and direction) for high-level view of these features
df = spark.sql("select * from forecast_explainer_master").toPandas().set_index(["sfdcAccountId","train_Date","bu","sub_bu","accountName","quarter_explained"])
columns_account = list(set(spark.sql("select * from forecast_explainer_master").columns).intersection(set(spark.sql("select old_name from ai.column_names where type='account' and shap_ind = 1").toPandas()["old_name"])))
columns_product = list(set(spark.sql("select * from forecast_explainer_master").columns).intersection(set(spark.sql("select old_name from ai.column_names where type='product' and shap_ind = 1").toPandas()["old_name"])))
columns_consumption = list(set(spark.sql("select * from forecast_explainer_master").columns).intersection(set(spark.sql("select old_name from ai.column_names where type='consumption' and shap_ind = 1").toPandas()["old_name"])))

account_df = df[columns_account]
account_df["abs_total_account"] = abs(account_df.iloc[:, :]).sum(axis = 1)
account_df.reset_index(inplace=True)

product_df = df[columns_product]
product_df["abs_total_product"] = abs(product_df.iloc[:, :]).sum(axis = 1)
product_df.reset_index(inplace=True)

consumption_df = df[columns_consumption]
consumption_df["abs_total_consumption"] = abs(consumption_df.iloc[:, :]).sum(axis = 1)
consumption_df.reset_index(inplace=True)

combined_df = account_df[["sfdcAccountId","train_Date","bu","sub_bu","accountName","quarter_explained","abs_total_account"]].merge(product_df[["sfdcAccountId","train_Date","bu","sub_bu","accountName","quarter_explained","abs_total_product"]], on = ["sfdcAccountId","train_Date","bu","sub_bu","accountName","quarter_explained"]).merge(consumption_df[["sfdcAccountId","train_Date","bu","sub_bu","accountName","quarter_explained","abs_total_consumption"]], on = ["sfdcAccountId","train_Date","bu","sub_bu","accountName","quarter_explained"])

combined_df["abs_total"] = abs(combined_df.iloc[:,6:9]).sum(axis = 1)
combined_df[["abs_total_account","abs_total_product","abs_total_consumption"]] = abs(combined_df.iloc[:,6:9]).div(combined_df["abs_total"].values, axis=0)
combined_df = combined_df.drop("abs_total", axis = 1)

spark.createDataFrame(combined_df).write.mode("overwrite").format("delta").saveAsTable("ai.high_level_shapley")


account_df = df[columns_account]
account_df["total_account"] = (account_df.iloc[:, :]).sum(axis = 1)
account_df.reset_index(inplace=True)

product_df = df[columns_product]
product_df["total_product"] = (product_df.iloc[:, :]).sum(axis = 1)
product_df.reset_index(inplace=True)

consumption_df = df[columns_consumption]
consumption_df["total_consumption"] = (consumption_df.iloc[:, :]).sum(axis = 1)
consumption_df.reset_index(inplace=True)


combined_df = account_df[["sfdcAccountId","train_Date","bu","sub_bu","accountName","quarter_explained","total_account"]].merge(product_df[["sfdcAccountId","train_Date","bu","sub_bu","accountName","quarter_explained","total_product"]], on = ["sfdcAccountId","train_Date","bu","sub_bu","accountName","quarter_explained"]).merge(consumption_df[["sfdcAccountId","train_Date","bu","sub_bu","accountName","quarter_explained","total_consumption"]], on = ["sfdcAccountId","train_Date","bu","sub_bu","accountName","quarter_explained"])
combined_df.set_index(["sfdcAccountId","train_Date","bu","sub_bu","accountName","quarter_explained"], inplace= True)
combined_df[combined_df > 0] = 1
combined_df[combined_df <= 0] = 0
combined_df.reset_index(inplace=True)

spark.sql("drop table ai.high_level_shapley_direction")
spark.createDataFrame(combined_df).write.mode("overwrite").format("delta").saveAsTable("ai.high_level_shapley_direction")
