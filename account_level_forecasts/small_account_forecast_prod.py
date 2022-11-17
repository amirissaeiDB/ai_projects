# Databricks notebook source
# MAGIC %md
# MAGIC ## Please do not write into prod tables. If you clone the notebook, please make sure to change the `database` and `table` names. Otherwise, the prod job will be corrupted. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explanation of the variables:
# MAGIC ##### table_name: name of the master table e.g. account_week_master
# MAGIC ##### db_name: name of the database where master table is in e.g. usage_forecasting_acct
# MAGIC ##### horizon: number of weeks we want to forecast
# MAGIC ##### output_folder: Where do we want to write the results? The table is created with the output_folder as part of its name. YOu can pick any names.
# MAGIC ##### backtest: 0 or 1. Is this a backtest or not?
# MAGIC ##### num_workers: Number of workers in Spark clusters. Used for XGboost training speed up
# MAGIC ##### use_gpu: 0 or 1. Does cluster have GPU? XGBoost uses GPU to speed up the training time. 

# COMMAND ----------

dbutils.widgets.text("db_name","usage_forecasting_acct")
dbutils.widgets.text("table_name","account_week_master")
dbutils.widgets.text("horizon","40")
dbutils.widgets.text("output_folder","backtest")
dbutils.widgets.text("backtest","1")
dbutils.widgets.text("num_workers","6")
dbutils.widgets.text("use_gpu","0")
db_name = dbutils.widgets.get("db_name")
output_folder = dbutils.widgets.get("output_folder")
table_name = dbutils.widgets.get("table_name")
horizon = int(dbutils.widgets.get("horizon"))
backtest = int(dbutils.widgets.get("backtest"))
num_workers = int(dbutils.widgets.get("num_workers"))
use_gpu = int(dbutils.widgets.get("use_gpu"))

# COMMAND ----------

#Enable Dleta cache
spark.conf.set("spark.databricks.io.cache.enabled", "true")
# We set any accounts with >8 weeks of no usage to 0. If they appear again, we will generate forecast
no_usage_accounts = spark.sql(f"select distinct sfdcAccountId from (select * from {db_name}.{table_name} where week = (select max(week) from  {db_name}.{table_name}) and (noUsageSince12WeeksAgo = 'true'  or real = 0) ) where endWeek<week-7")
no_usage_accounts.createOrReplaceTempView("not_active_accounts")
spark.sql("drop table if exists ai.not_active_accounts")
spark.sql("create table ai.not_active_accounts select * from not_active_accounts")

# COMMAND ----------

from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator
from pyspark.sql.functions import col,lit
from sparkdl.xgboost import XgboostRegressor
import numpy as np
import uuid

# inference data
def get_inference_data(type, week):
  if type == 'no_usage':
    df = spark.sql(f"select *, endWeek-startWeek as usage_duration, week-startWeek as week_from_first_use from {db_name}.{table_name} where noUsageSince12WeeksAgo = 'true' and weekStart ={week}")
  else:
    df = spark.sql(f"select *, endWeek-startWeek as usage_duration, week-startWeek as week_from_first_use from {db_name}.{table_name} where sfdcAccountId not in (select * from ai.not_active_accounts) and weekStart ={week} and (noUsageSince12WeeksAgo or real =0)")
  return df

def get_train_data(type):
  
  if type == 'no_usage':
    df = spark.sql(f"select * from {db_name}.{table_name}  where noUsageSince12WeeksAgo = 'true'")
  else:
    df = spark.sql(f"select * from {db_name}.{table_name}  where sfdcAccountId not in (select * from ai.not_active_accounts) and (noUsageSince12WeeksAgo or real =0)")
  pandas_df = df.toPandas()
  pandas_df = pandas_df.replace([np.inf, -np.inf], np.nan)
  spark_df = spark.createDataFrame(pandas_df)
  return spark_df


# train the model
def train_model(inference_data, type, horizon, output_folder, as_of_date,inference_date, num_workers, use_gpu):
  df = get_train_data(type)
  df.createOrReplaceTempView("small_accounts_view")
  # unique uuid to make sure the models are written to distinct directories 
  uuidOne = uuid.uuid1()
  uuid_str = str(uuidOne).replace("-","_")
  # build one xgboost model for each step
  for i in range(1,horizon + 1,1):
    print(i)
    # create the training data on the fly
    df = spark.sql(f"select a.*,a.endWeek-a.startWeek as usage_duration, a.week-a.startWeek as week_from_first_use, (b.dollars-a.dollars) as target from small_accounts_view as a inner join small_accounts_view as b on a.sfdcAccountId = b.sfdcAccountId and a.week = b.week-{i} and a.weekStart != {inference_date}")
    df.repartition(4).cache().count()
    
    #cateogrical columns are selected 
    categoricalCols = ['region','l1segment','l2segment','l3segment','sfdc_region_l1','sfdc_region_l2','sfdc_region_l3','bu','vertical','account_status_imputed','scp_status_imputed','scp_tier_imputed','scp_budgeted_imputed','industry_vertical_imputed','industry_group_imputed','industry_imputed','employee_range_imputed','annual_revenue_range_imputed','cse_tier_imputed']
    indexOutputCols = [x + "Index" for x in categoricalCols]
    stringIndexer = StringIndexer(inputCols=categoricalCols, outputCols=indexOutputCols, handleInvalid="keep")
    
    # all PCA components and exclude the target
    numericCols = list(set([field for (field, dataType) in df.dtypes if dataType != "string"])- set(['weekStart','noUsageSince12WeeksAgo','real','isRealCustomerAsOf','nonpvc','pvc','pubsec','top100','target']))
    assemblerInputs =  numericCols + indexOutputCols
    vecAssembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features", handleInvalid= "keep")
    
    # xgboost in Spark
    if use_gpu == 1:
      use_gpu = True
    else:
      use_gpu = False
    xgb_regressor = XgboostRegressor(num_workers = num_workers, labelCol="target", missing = 0.0, use_gpu = use_gpu)
    paramGrid = ParamGridBuilder()\
    .addGrid(xgb_regressor.max_depth, [3, 5])\
    .addGrid(xgb_regressor.n_estimators, [10, 20])\
    .build()
    
    # 3-folds cross validaiton to tune the HPs
    evaluator = RegressionEvaluator(labelCol="target", predictionCol="prediction")
    cv = CrossValidator(estimator=xgb_regressor, evaluator=evaluator, estimatorParamMaps=paramGrid, numFolds=3, parallelism= 4, seed=42)

    stagesWithCV = [stringIndexer, vecAssembler, cv] 
    pipeline = Pipeline(stages=stagesWithCV)
    pipelineModel = pipeline.fit(df)
    
    # Save the pipeline for later use. 
    # TODO: replace this with mlflow
    pipelineModel.save(f"dbfs:/amir/account_forecasting/xgboost/small_accounts/{output_folder}_{str(date.today())}_{uuid_str}/week_{i}")
    predDF = pipelineModel.transform(inference_data)
    
    # create the final data as a Delta table
    prediction = predDF.select(col("sfdcAccountId"),col("prediction"),col("dollars"),lit(i).alias("week_ahead"),lit(as_of_date).alias("train_data_as_of"))
    (prediction.write.format("delta")
      .mode("append")
      .saveAsTable(f"ai.{output_folder}_small_account_forecast"))

# COMMAND ----------

# not real customers
# kick off the training for not_real customers
from datetime import date
inference_date = "'"+str(spark.sql(f"select max(weekStart) as as_of_date from {db_name}.{table_name}").first()[0])+"'"
inference_data = get_inference_data("not_real",inference_date)
if not backtest:
  output_folder_not_real = 'not_real_no_pca_diff'
else:
  output_folder_not_real = output_folder
  dbutils.fs.rm(f"dbfs:/amir/account_forecasting/xgboost/small_accounts/{output_folder_not_real}/",True)
  spark.sql(f"drop table if exists ai.{output_folder}_small_account_forecast")
as_of_date = "'"+str(spark.sql(f"select max(weekStart)+7 as inference_date from usage_forecasting_acct.account_week_master").first()[0])+"'"
type = 'not_real'
train_model(inference_data, type, horizon, output_folder_not_real, as_of_date, inference_date, num_workers, use_gpu)

# COMMAND ----------

if not backtest:
  spark.sql(f"insert into usage_forecasting_prod.small_account_forecast with forecasted_accounts as (\
      select\
        sfdcAccountId,\
        case when prediction+dollars<0 then 0 else prediction+dollars end as forecast,\
        date_add({inference_date}, 7 * week_ahead) as week,\
        cast(replace({as_of_date}, \"'\", \"\") as date) as train_date\
      from\
        ai.not_real_no_pca_diff_small_account_forecast where cast(replace(train_data_as_of, \"'\", \"\") as date) = {as_of_date}\
    ),\
    not_active_accounts as (\
    select a.sfdcAccountId, 0 as forecast, b.week, b.train_date from ai.not_active_accounts as a cross join (select distinct week, train_date from forecasted_accounts as b where train_date = {as_of_date})b\
                   )\
    select\
      *\
    from\
      forecasted_accounts union all select * from not_active_accounts"\
           )
else:
  spark.sql(f"drop table if exists usage_forecasting_prod.{output_folder}_small_account_forecast")
  spark.sql(f"create table usage_forecasting_prod.{output_folder}_small_account_forecast with forecasted_accounts as (\
      select\
        sfdcAccountId,\
        case when prediction+dollars<0 then 0 else prediction+dollars end as forecast,\
        date_add({inference_date}, 7 * week_ahead) as week,\
        cast(replace({as_of_date}, \"'\", \"\") as date) as train_date\
      from\
        ai.{output_folder}_small_account_forecast where cast(replace(train_data_as_of, \"'\", \"\") as date) = {as_of_date}\
    ),\
    not_active_accounts as (\
    select a.sfdcAccountId, 0 as forecast, b.week, b.train_date from ai.not_active_accounts as a cross join (select distinct week, train_date from forecasted_accounts as b where train_date = {as_of_date})b\
                   )\
    select\
      *\
    from\
      forecasted_accounts union all select * from not_active_accounts"\
           )

# COMMAND ----------

# # no usage customers
# # # kick off the training for no_usage customers

# inference_date = "'"+str(spark.sql("select max(weekStart) as inference_date from usage_forecasting_acct.account_week_master").first()[0])+"'"
# inference_data = get_inference_data('no_usage',inference_date)
# horizon = 52
# path = "dbfs:/amir/small_account/data/no_usage"
# output_folder_no_usage = 'no_usage_no_pca_diff_2'
# as_of_date = "'"+str(date.today())+"'"
# type = 'no_usage'
# train_model(inference_data, type, horizon, output_folder_no_usage, as_of_date, inference_date)
