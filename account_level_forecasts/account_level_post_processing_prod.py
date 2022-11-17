# Databricks notebook source
# MAGIC %md
# MAGIC ## Please do not write into prod tables. If you clone the notebook, please make sure to change the `database` and `table` names. Otherwise, the prod job will be corrupted. 

# COMMAND ----------

dbutils.widgets.text("db_name","usage_forecasting_acct")
dbutils.widgets.text("table_name","account_week_master")
dbutils.widgets.text("output_folder","backtest")
dbutils.widgets.text("backtest","1")
db_name = dbutils.widgets.get("db_name")
table_name = dbutils.widgets.get("table_name")
output_folder = dbutils.widgets.get("output_folder")
backtest = int(dbutils.widgets.get("backtest"))

#Enable Dleta cache
spark.conf.set("spark.databricks.io.cache.enabled", "true")

# COMMAND ----------

from pyspark.sql.functions import add_months

if not backtest:
  
  #inference date to filter the data
  inference_date = "'"+str(spark.sql("select max(weekStart) as inference_date from usage_forecasting_acct.account_week_master").first()[0])+"'"
  end_date = "'"+str(spark.sql("select max(week) as end_week from usage_forecasting_prod.real_account_forecast").first()[0])+"'"

  # optmize tables to maek sure the queries run fast
  spark.sql("optimize usage_forecasting_prod.real_account_forecast")
  spark.sql("optimize usage_forecasting_prod.small_account_forecast")
  spark.sql("optimize usage_forecasting_prod.account_pvc_forecast_ai_pipeline")

  # Add small, real and pvc data together
  df = spark.sql("select sfdcAccountId, week, train_date, sum(forecast) as forecast from usage_forecasting_prod.real_account_forecast group by sfdcAccountId, week, train_date union all select sfdcAccountId, week, train_date, sum(forecast) as forecast from usage_forecasting_prod.small_account_forecast group by sfdcAccountId, week, train_date union all select sfdcAccountId, week, train_date, sum(forecast) as forecast from usage_forecasting_prod.account_pvc_forecast_ai_pipeline group by sfdcAccountId, week, train_date")

  # Aggreagte per acocunt, per week (affects PVC customers with non-PVC usage)
  df = df.groupBy("sfdcAccountId","week","train_date").sum().withColumnRenamed("sum(forecast)","forecast")

  # Filter the extra data
  train_date = "'"+str(spark.sql("select max(train_date) as m from usage_forecasting_prod.real_account_forecast").first()[0]) + "'"
  df_filtered = df.where(f"week>{inference_date} and week<={end_date} and train_date = {train_date}")

  #append to the production table
  df_filtered.write.format("delta").mode("append").saveAsTable("usage_forecasting_prod.account_level_forecast_prod_ai_pipeline")
else:
  #inference date to filter the data
  inference_date = "'"+str(spark.sql(f"select max(weekStart) as inference_date from {db_name}.{table_name}").first()[0])+"'"
  end_date = "'"+str(spark.sql(f"select max(week) as end_week from usage_forecasting_prod.{output_folder}_real_account_forecast").first()[0])+"'"

  # optmize tables to maek sure the queries run fast
  spark.sql(f"optimize usage_forecasting_prod.{output_folder}_real_account_forecast")
  spark.sql(f"optimize usage_forecasting_prod.{output_folder}_small_account_forecast")
  spark.sql(f"optimize usage_forecasting_prod.{output_folder}_pvc_forecast")

  # Add small, real and pvc data together
  df = spark.sql(f"select sfdcAccountId, week, train_date, sum(forecast) as forecast from usage_forecasting_prod.{output_folder}_real_account_forecast group by sfdcAccountId, week, train_date union all select sfdcAccountId, week, train_date, sum(forecast) as forecast from usage_forecasting_prod.{output_folder}_small_account_forecast group by sfdcAccountId, week, train_date union all select sfdcAccountId, week, train_date, sum(forecast) as forecast from usage_forecasting_prod.{output_folder}_pvc_forecast group by sfdcAccountId, week, train_date")

  # Aggreagte per acocunt, per week (affects PVC customers with non-PVC usage)
  df = df.groupBy("sfdcAccountId","week","train_date").sum().withColumnRenamed("sum(forecast)","forecast")

  # Filter the extra data
  train_date = "'"+ str(spark.sql(f"select max(train_date) as m from usage_forecasting_prod.{output_folder}_real_account_forecast").first()[0]) + "'"
  df_filtered = df.where(f"week>{inference_date} and week<={end_date} and train_date = {train_date}")

  #append to the production table
  df_filtered.write.format("delta").mode("overwrite").saveAsTable("usage_forecasting_prod.account_level_forecast_backtest")

# COMMAND ----------

# Expential moving average

from pyspark.sql.functions import pandas_udf, lit
from pyspark.sql.functions import PandasUDFType
from pyspark.sql.types import DoubleType, StructField
def ema(pdf):
    pdf = pdf.sort_values("train_date")
    pdf['ewm'] = pdf['forecast'].ewm(span=9, min_periods=1, adjust=False).mean()
    return pdf

if not backtest:
  # Read all the data. This inclues historical runs.
  df = spark.sql("select * from usage_forecasting_prod.account_level_forecast_prod_ai_pipeline").withColumn("ewm",lit(0.0))
  schema = df.schema

  # Apply Pandas UDF per account per week. 
  df_ewa = df.groupby('sfdcAccountId','week').applyInPandas(ema, schema).select("sfdcAccountId", "week", "train_date", "ewm").withColumnRenamed("ewm", "forecast")
  train_date = str(spark.sql("select max(train_date) as m from usage_forecasting_prod.real_account_forecast").toPandas()["m"][0])
  df_ewa.write.format("delta").mode("overwrite").saveAsTable("usage_forecasting_prod.smoothed_account_level_forecast_with_history")
  df_ewa.where(f"train_date = '{train_date}'").write.format("delta").mode("append").saveAsTable("usage_forecasting_prod.smoothed_account_level_forecast")
else:
  df = spark.sql(f"select * from usage_forecasting_prod.account_level_forecast_backtest").withColumn("ewm",lit(0.0))
  schema = df.schema
  # Apply Pandas UDF per account per week. 
  df_ewa = df.groupby('sfdcAccountId','week').applyInPandas(ema, schema).select("sfdcAccountId", "week", "train_date", "ewm").withColumnRenamed("ewm", "forecast")
  train_date = spark.sql(f"select max(train_date) as m from usage_forecasting_prod.{output_folder}_real_account_forecast").toPandas()["m"][0]
  df_ewa.write.format("delta").mode("overwrite").saveAsTable("usage_forecasting_prod.smoothed_account_level_forecast_with_history_backtest")
  df_ewa.where(f"train_date = '{train_date}'").write.format("delta").mode("overwrite").saveAsTable("usage_forecasting_prod.smoothed_account_level_forecast_backtest")

# COMMAND ----------

# Changed data to daily forecast based on aggregate data: find total $DBU for the company in last 370 days and find proportion of DOW

if not backtest:
  df = spark.sql("with dow_portion as ( select dayofweek(date) as dow, sum(dbuDollars) as actual from finance.dbu_dollars where date>= date_add(current_date(),-370) and date<current_date() group by dayofweek(date)), portion as (select dow, actual/total as portion from dow_portion cross join (select sum(dbuDollars) as  total from finance.dbu_dollars where date>= date_add(current_date(),-370) and date<current_date())), calendar as (SELECT explode(sequence(to_date('2010-01-01'), to_date('2030-12-31'), interval 1 day)) as date ), daily_forecast as ( select  a.sfdcAccountId,b.date,a.train_date, a.forecast*c.portion as forecast from usage_forecasting_prod.account_level_forecast_prod_ai_pipeline as a left join calendar as b on b.date between a.week and a.week+6 left join portion as c on dayofweek(b.date) = c.dow) select * from daily_forecast")
  df.write.format("delta").mode("overwrite").saveAsTable("usage_forecasting_prod.account_level_ds_daily_forecast_ai_pipeline")
else:
  df = spark.sql("with dow_portion as ( select dayofweek(date) as dow, sum(dbuDollars) as actual from finance.dbu_dollars where date>= date_add(current_date(),-370) and date<current_date() group by dayofweek(date)), portion as (select dow, actual/total as portion from dow_portion cross join (select sum(dbuDollars) as  total from finance.dbu_dollars where date>= date_add(current_date(),-370) and date<current_date())), calendar as ( SELECT explode(sequence(to_date('2010-01-01'), to_date('2030-12-31'), interval 1 day)) as date ), daily_forecast as ( select  a.sfdcAccountId,b.date,a.train_date, a.forecast*c.portion as forecast from usage_forecasting_prod.account_level_forecast_backtest as a left join calendar as b on b.date between a.week and a.week+6 left join portion as c on dayofweek(b.date) = c.dow) select * from daily_forecast")
  df.write.format("delta").mode("overwrite").saveAsTable("usage_forecasting_prod.account_level_ds_forecast_daily_backtest")

# COMMAND ----------

# Expential moving average
def ema(pdf):
    pdf = pdf.sort_values("train_date")
    pdf['ewm'] = pdf['forecast'].ewm(span=9, min_periods=1, adjust=False).mean()
    return pdf

if not backtest:
  # Read all the data. This inclues historical runs.
  df = spark.sql("select * from usage_forecasting_prod.account_level_ds_daily_forecast_ai_pipeline").withColumn("ewm",lit(0.0))
  schema = df.schema
  # Apply Pandas UDF per account per week. 
  df_ewa = df.groupby('sfdcAccountId','date').applyInPandas(ema, schema).select("sfdcAccountId", "date", "train_date", "ewm").withColumnRenamed("ewm", "forecast")
  train_date = spark.sql("select max(train_date) as m from usage_forecasting_prod.real_account_forecast").toPandas()["m"][0]
  df_ewa.write.format("delta").mode("overwrite").saveAsTable("usage_forecasting_prod.smoothed_account_level_ds_daily_forecast_with_history")
  df_ewa.where(f"train_date = '{train_date}'").write.format("delta").mode("append").saveAsTable("usage_forecasting_prod.smoothed_account_level_ds_daily_forecast")
else:
  df = spark.sql(f"select * from usage_forecasting_prod.account_level_ds_forecast_daily_backtest").withColumn("ewm",lit(0.0))
  schema = df.schema
  # Apply Pandas UDF per account per week. 
  df_ewa = df.groupby('sfdcAccountId','date').applyInPandas(ema, schema).select("sfdcAccountId", "date", "train_date", "ewm").withColumnRenamed("ewm", "forecast")
  train_date = spark.sql(f"select max(train_date) as m from usage_forecasting_prod.{output_folder}_real_account_forecast").toPandas()["m"][0]
  df_ewa.write.format("delta").mode("overwrite").saveAsTable("usage_forecasting_prod.smoothed_account_level_ds_faily_forecast_with_history_backtest")
  df_ewa.where(f"train_date = '{train_date}'").write.format("delta").mode("overwrite").saveAsTable("usage_forecasting_prod.smoothed_account_level_ds_forecast_daily_backtest")

# COMMAND ----------

# # Add actuals to the daily tables

# actual_end_date = "'"+str(spark.sql("select max(weekStart)+7 as actual_end_date from usage_forecasting_acct.account_week_master").first()[0])+"'"
# df_actual = spark.sql(f"with non_pvc_actuals AS (SELECT accountID as sfdcAccountId, date, sum(dbuDollars) as dbuDollars from finance.dbu_dollars group by accountID, date), calendar as ( SELECT explode(sequence(to_date('2010-01-01'), to_date('2030-12-31'), interval 1 day)) AS date ), pvc_actual as (select sfdcAccountId, date, sum(dbuDollars) as dbuDollars from (select accountID as sfdcAccountId, last_day(make_date(year, month, 1)) as date, dbuDollars from finance.pvc_usage union all select accountID as sfdcAccountId, last_day(make_date(year, month, 1)) as date, dbuDollars from finance.pubsec_usage) group by sfdcAccountId, date) select a.sfdcAccountId, b.date, {train_date} as train_date, a.dbuDollars/extract(day from a.date) as dbuDollars from pvc_actual as a left join calendar as b on extract(month from b.date) = extract(month from a.date) and extract(year from b.date) = extract(year from a.date) where b.date<{actual_end_date} union all select sfdcAccountId, date, {train_date} as train_date, dbuDollars from non_pvc_actuals where date<{actual_end_date}")

