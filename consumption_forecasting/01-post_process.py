# Databricks notebook source
horizon = 124
f = open('/dbfs/amir/output_table_name.txt', 'r')
output_table_name= f.read()
forecast_as_of_str = output_table_name.replace("_","-")

# COMMAND ----------

#Orbit run does not include MSFT. We are rolling BU back to AMER Ent
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('temps-demo').config("spark.driver.bindAddress", "127.0.0.1").getOrCreate()

if spark.sql(f"select count(*) from ai_2.{output_table_name}").first()[0] == 7*horizon and spark.sql(f"select count(*) from usage_forecasting_prod.forecast_results where forecast_as_of = '{forecast_as_of_str}'").first()[0] != 0 :
  
  spark.sql(f"insert into usage_forecasting_prod.forecast_all_model_results (select concat(regexp_replace(region,\"'\",\"\"), regexp_replace(segment,\"'\",\"\"),'y') as ts_key, 'Greykite' as model, 'na' as run_id, cast(ts as date) as date, forecast as y, forecast_lower as y_lower, forecast_upper as y_upper, date_trunc('hour', `timestamp`) as train_run_date, 'na' as train_run_uuid,date_trunc('hour', `timestamp`) as score_run_date, '{forecast_as_of_str}' as forecast_as_of, 'na' as deployment_uuid, -1 as table_version from ai_2.{output_table_name})")
  train_run_date = spark.sql(f"select distinct train_run_date from usage_forecasting_prod.forecast_all_model_results where forecast_as_of = '{forecast_as_of_str}' and model != 'Greykite' order by train_run_date desc")
  train_run_date_str = str(train_run_date.first()[0])
  spark.sql(f"delete from usage_forecasting_prod.forecast_results where forecast_as_of = '{forecast_as_of_str}'")
  spark.sql(f"insert into usage_forecasting_prod.forecast_results(select ts_key, date, 'na' as train_run_uuid,  '{train_run_date_str}' as train_run_date,  '{train_run_date_str}' as score_run_date, '{forecast_as_of_str}' as forecast_as_of, y, y_lower, y_upper, 'true' as is_active, 'na' as deployment_uuid, -1 as table_version from  ( select ts_key, date, avg(y) as y, avg(y_lower) as y_lower, avg(y_upper) as y_upper from (select ts_key, date, model, forecast_as_of, sum(y) as y, sum(y_lower) as y_lower, sum(y_upper) as y_upper from (select case when ts_key='MSFTMSFTy' then 'AMEREnterprisey' else ts_key end as ts_key, date, model, y, y_lower, y_upper, forecast_as_of from usage_forecasting_prod.forecast_all_model_results) group by ts_key, date, model, forecast_as_of) where forecast_as_of = '{forecast_as_of_str}' group by ts_key, date ))")
  spark.sql(f"delete from usage_forecasting_tab_silver.frozen_actuals_non_pvc_pub where forecast_date = '{forecast_as_of_str}'")
else:
  raise ValueError('Orbit code had issue. Fix the issue upstream.')

# COMMAND ----------

# Create a final table for look up

from datetime import date
from datetime import timedelta
from pyspark.sql.functions import lit

latest_forecast_run_date = spark.sql("select max(forecast_as_Of) as latest_forecast_run from usage_forecasting_prod.forecast_results").toPandas()["latest_forecast_run"][0]
month = latest_forecast_run_date.month
year = latest_forecast_run_date.year
day = latest_forecast_run_date.day

if month>=2 and month<=4:
  q_start = str(year) + '-' + '02' + '-' + '01'
  q_end = str(year) + '-' + '04' + '-' + '30'
if month>=5 and month<=7:
  q_start = str(year) + '-' + '05' + '-' + '01'
  q_end = str(year) + '-' + '07' + '-' + '31'
if month>=8 and month<=10:
  q_start = str(year) + '-' + '08' + '-' + '01'
  q_end = str(year) + '-' + '10' + '-' + '31'
if month>=11 and month<=12:
  q_start = str(year) + '-' + '11' + '-' + '01'
  q_end = str(year+1) + '-' + '01' + '-' + '31'
if month==1:
  q_start = str(year-1) + '-' + '11' + '-' + '01'
  q_end = str(year) + '-' + '01' + '-' + '31'

if day<=10:
  if month==1:
    pvc_end_date = str(year-1)+'-'+str(11)+'-'+'01'
    pvc_forecast_as_of = str(year-1)+'-'+str(12)+'-'+'01'
  elif month==2:
    pvc_end_date = str(year-1)+'-'+str(12)+'-'+'01'
    pvc_forecast_as_of = str(year)+'-'+str(1)+'-'+'01'
  else:
    pvc_end_date = str(year)+'-'+str(month-2)+'-'+'01'
    pvc_forecast_as_of = str(year)+'-'+str(month-1)+'-'+'01'
else:
  if month==1:
    pvc_end_date = str(year-1)+'-'+str(12)+'-'+'01'
    pvc_forecast_as_of = str(year)+'-'+str(month)+'-'+'01'
  else:
    pvc_end_date = str(year)+'-'+str(month-1)+'-'+'01'
    pvc_forecast_as_of = str(year)+'-'+str(month)+'-'+'01'
  
non_pvc_forecast = spark.sql(f"select substr(ts_key, 1, length(ts_key)-1) as ts_key,ct, y,y_lower, y_upper from (select ts_key, count(*) as ct, sum(y) as y, sum(y_lower) as y_lower, sum(y_upper) as y_upper from usage_forecasting_prod.forecast_results where forecast_as_of = '{latest_forecast_run_date}' and is_active = 'true' and date>='{latest_forecast_run_date}' and date<='{q_end}' group by ts_key)")

table_version = spark.sql(f"select distinct table_version from usage_forecasting_prod.forecast_all_model_results where forecast_as_Of = '{latest_forecast_run_date}' and model != 'Greykite'").first()[0]

non_pvc_actual = spark.sql(f"select ts_key, count(*) as ct, sum(y) as y, sum(y) as y_lower, sum(y) as y_upper from (select concat(region, segment) as ts_key, dbudollars as y from (select region, case when region in ('EMEA','APJ') then 'ALL' else l1segment end as segment, dbudollars from finance.dbu_dollars version as of {table_version} where date>='{q_start}' and date<'{latest_forecast_run_date}')) group by ts_key ")

non_pvc_actual_msft = spark.sql(f"select 'MSFTMSFT' as ts_key, ct, y, y_lower, y_upper from (select ts_key, count(*) as ct, sum(y) as y, sum(y) as y_lower, sum(y) as y_upper from (select concat(region, segment) as ts_key, dbudollars as y from (select region, case when region in ('EMEA','APJ') then 'ALL' else l1segment end as segment, dbudollars from finance.dbu_dollars version as of {table_version} where date>='{q_start}' and date<'{latest_forecast_run_date}' and accountID='00161000005eOdjAAE')) group by ts_key)")

non_pvc_forecast_msft = spark.sql(f"select substr(ts_key, 1, length(ts_key)-1) as ts_key,ct, y,y_lower, y_upper from (select ts_key, count(*) as ct, sum(y) as y, sum(y_lower) as y_lower, sum(y_upper) as y_upper from usage_forecasting_prod.forecast_all_model_results where forecast_as_of = '{latest_forecast_run_date}' and date>='{latest_forecast_run_date}' and date<='{q_end}' and model = 'Greykite' and ts_key = 'MSFTMSFTy' group by ts_key)")

                        
pvc_actual = spark.sql(f"select concat(region, segment) as ts_key, count(*) as ct, sum(dbudollars) as y, sum(dbudollars) as y_lower, sum(dbudollars) as y_upper from (select region, case when region in ('EMEA','APJ') then 'ALL' else l1segment end as segment, make_date(year, month, 1) as date, dbudollars from finance.pvc_usage where make_date(year, month, 1)>='{q_start}' and make_date(year, month, 1)<='{pvc_end_date}' union all (select region, case when region in ('EMEA','APJ') then 'ALL' else l1segment end as segment, make_date(year, month, 1) as date, dbudollars from finance.pubsec_usage where make_date(year, month, 1)>='{q_start}' and make_date(year, month, 1)<='{pvc_end_date}')) group by concat(region, segment)")


pvc_forecast= spark.sql(f"select substr(ts_key,1,length(ts_key)-1) as ts_key,count(*) as ct, sum(y) as y, sum(y_lower) as y_lower, sum(y_upper) as y_upper from usage_forecasting_prod.forecast_results_pvc where forecast_as_of = '{pvc_forecast_as_of}' and date>'{pvc_end_date}' and date<= '{q_end}' and is_active = 'true' group by substr(ts_key,1,length(ts_key)-1)")
  
forecast_actual = non_pvc_forecast.select("ts_key", "ct", "y", "y_lower", "y_upper").unionAll(non_pvc_actual.select("ts_key", "ct", "y", "y_lower", "y_upper")).unionAll(pvc_actual.select("ts_key", "ct", "y", "y_lower", "y_upper")).unionAll(pvc_forecast.select("ts_key", "ct", "y", "y_lower", "y_upper")).unionAll(non_pvc_forecast_msft.select("ts_key", "ct", "y", "y_lower", "y_upper")).unionAll(non_pvc_actual_msft.select("ts_key", "ct", "y", "y_lower", "y_upper")).withColumnRenamed("ts_key","BU").withColumnRenamed("y","forecast").withColumnRenamed("y_lower","forecast_lower").withColumnRenamed("y_upper","forecast_upper")

snapshot_tableau = forecast_actual.select("BU","forecast","forecast_lower","forecast_upper").groupBy("BU").sum().withColumnRenamed("sum(forecast)","forecast").withColumnRenamed("sum(forecast_lower)","forecast_lower").withColumnRenamed("sum(forecast_upper)","forecast_upper")

snapshot_tableau.withColumn("forecast_as_of", lit(latest_forecast_run_date)).write.format("delta").mode("append").saveAsTable(f"usage_forecasting_prod.ai_tableau_snapshot")
