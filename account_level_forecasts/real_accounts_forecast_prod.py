# Databricks notebook source
# MAGIC %md
# MAGIC ## Please do not write into prod tables. If you clone the notebook, please make sure to change the `database` and `table` names. Otherwise, the prod job will be corrupted. 

# COMMAND ----------

dbutils.widgets.text("db_name","usage_forecasting_acct")
dbutils.widgets.text("table_name","account_week_master")
dbutils.widgets.text("horizon","40")
dbutils.widgets.text("output_folder","backtest_real")
dbutils.widgets.text("backtest","1")
dbutils.widgets.text("num_workers","6")
dbutils.widgets.text("use_gpu","1")
db_name = dbutils.widgets.get("db_name")
use_gpu = int(dbutils.widgets.get("use_gpu"))
output_folder = dbutils.widgets.get("output_folder")
num_workers = int(dbutils.widgets.get("num_workers"))
table_name = dbutils.widgets.get("table_name")
horizon = int(dbutils.widgets.get("horizon"))
backtest = int(dbutils.widgets.get("backtest"))

#Enable Dleta cache
spark.conf.set("spark.databricks.io.cache.enabled", "true")

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
def get_inference_data(week):
  df = spark.sql(f"select *, endWeek-startWeek as usage_duration, week-startWeek as week_from_first_use from {db_name}.{table_name} where !(noUsageSince12WeeksAgo or real=0) and weekStart ={week}")
  return df

def get_train_data():
  df = spark.sql(f"select * from {db_name}.{table_name} where !(noUsageSince12WeeksAgo or real=0)")
  pandas_df = df.toPandas()
  pandas_df = pandas_df.replace([np.inf, -np.inf], np.nan)
  spark_df = spark.createDataFrame(pandas_df)
  return spark_df


# train the model
def train_model(inference_data, horizon, output_folder, as_of_date,inference_date, num_workers, use_gpu ): 
  df = get_train_data()
  df.createOrReplaceTempView("real_accounts_view")
  uuidOne = uuid.uuid1()
  uuid_str = str(uuidOne).replace("-","_")
  # build one xgboost model for each step
  for i in range(1,horizon + 1,1):
    print(i)
    # create the training data on the fly
    df = spark.sql(f"select a.*,a.endWeek-a.startWeek as usage_duration, a.week-a.startWeek as week_from_first_use, (b.dollars-a.dollars) as target from real_accounts_view as a inner join real_accounts_view as b on a.sfdcAccountId = b.sfdcAccountId and a.week = b.week-{i} and a.weekStart != {inference_date}")
    df.repartition(2).cache().count()
    
    #cateogrical columns are selected 
    categoricalCols = ['region','l1segment','l2segment','l3segment','sfdc_region_l1','sfdc_region_l2','sfdc_region_l3','bu','vertical','account_status_imputed','scp_status_imputed','scp_tier_imputed','scp_budgeted_imputed','industry_vertical_imputed','industry_group_imputed','industry_imputed','employee_range_imputed','annual_revenue_range_imputed','cse_tier_imputed']
    indexOutputCols = [x + "Index" for x in categoricalCols]
    stringIndexer = StringIndexer(inputCols=categoricalCols, outputCols=indexOutputCols, handleInvalid="keep")
    
    # all PCA components and exclude the target
    numericCols = list(set([field for (field, dataType) in df.dtypes if dataType != "string"])- set(['weekStart','noUsageSince12WeeksAgo','real','isRealCustomerAsOf','nonpvc','pvc','pubsec','top100','target']))
    assemblerInputs =  numericCols + indexOutputCols
    vecAssembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features", handleInvalid= "keep")
    
    # xgboost in Spark
    if use_gpu==1:
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
    cv = CrossValidator(estimator=xgb_regressor, evaluator=evaluator, estimatorParamMaps=paramGrid, 
                      numFolds=3, parallelism=2, seed=42)

    stagesWithCV = [stringIndexer, vecAssembler, cv] 
    pipeline = Pipeline(stages=stagesWithCV)
    pipelineModel = pipeline.fit(df)
    
    # Save the pipeline for later use. 
    # TODO: replace this with mlflow
    pipelineModel.save(f"dbfs:/amir/account_forecasting/xgboost/real_accounts/{output_folder}_{str(date.today())}_{uuid_str}/week_{i}")
    predDF = pipelineModel.transform(inference_data)
    
    # create the final data as a Delta table
    prediction = predDF.select(col("sfdcAccountId"),col("prediction"),col("dollars"),lit(i).alias("week_ahead"),lit(as_of_date).alias("train_data_as_of"))
    (prediction.write.format("delta")
      .mode("append")
      .saveAsTable(f"ai.{output_folder}_real_account_forecast"))

# COMMAND ----------

# not real customers
# kick off the training for not_real customers
from datetime import date
inference_date = "'"+str(spark.sql(f"select max(weekStart) as inference_date from {db_name}.{table_name}").first()[0])+"'"
inference_data = get_inference_data(inference_date)
if not backtest:
  output_folder_real = 'real_no_pca_diff'
else:
  output_folder_real = output_folder
  dbutils.fs.rm(f"dbfs:/amir/account_forecasting/xgboost/real_accounts/{output_folder_real}/",True)
  spark.sql(f"drop table if exists ai.{output_folder}_real_account_forecast")
as_of_date = "'"+str(spark.sql(f"select max(weekStart)+7 as as_of_date from usage_forecasting_acct.account_week_master").first()[0])+"'"
train_model(inference_data, horizon, output_folder_real, as_of_date, inference_date, num_workers, use_gpu)

# COMMAND ----------

if not backtest:  
  spark.sql(f"insert into usage_forecasting_prod.real_account_forecast with forecasted_accounts as (\
      select\
        sfdcAccountId,\
        case when prediction+dollars<0 then 0 else prediction+dollars end as forecast,\
        date_add({inference_date}, 7 * week_ahead) as week,\
        cast(replace({as_of_date}, \"'\", \"\") as date) as train_date\
      from\
        ai.real_no_pca_diff_real_account_forecast where cast(replace(train_data_as_of, \"'\", \"\") as date) = {as_of_date}\
    )\
    select\
      *\
    from\
      forecasted_accounts"\
           )
else:
  spark.sql(f"drop table if exists usage_forecasting_prod.{output_folder}_real_account_forecast")
  spark.sql(f"create table usage_forecasting_prod.{output_folder}_real_account_forecast with forecasted_accounts as (\
      select\
        sfdcAccountId,\
        case when prediction+dollars<0 then 0 else prediction+dollars end as forecast,\
        date_add({inference_date}, 7 * week_ahead) as week,\
        cast(replace({as_of_date}, \"'\", \"\") as date) as train_date\
      from\
        ai.{output_folder}_real_account_forecast where cast(replace(train_data_as_of, \"'\", \"\") as date) = {as_of_date}\
    )\
    select\
      *\
    from\
      forecasted_accounts")
