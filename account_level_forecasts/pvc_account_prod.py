# Databricks notebook source
# MAGIC %md
# MAGIC ## Please do not write into prod tables. If you clone the notebook, please make sure to change the `database` and `table` names. Otherwise, the prod job will be corrupted. 

# COMMAND ----------

dbutils.widgets.text("finance_db_name","finance")
dbutils.widgets.text("table_name_pubsec","pubsec_usage")
dbutils.widgets.text("table_name_pvc","pvc_usage")
finance_db_name = dbutils.widgets.get("finance_db_name")
dbutils.widgets.text("horizon_pvc","12")
dbutils.widgets.text("output_folder","backtest")
dbutils.widgets.text("backtest","1")
table_name_pubsec = dbutils.widgets.get("table_name_pubsec")
table_name_pvc = dbutils.widgets.get("table_name_pvc")
output_folder = dbutils.widgets.get("output_folder")
horizon = int(dbutils.widgets.get("horizon_pvc"))
backtest = int(dbutils.widgets.get("backtest"))

#Enable Dleta cache
spark.conf.set("spark.databricks.io.cache.enabled", "true")

# COMMAND ----------

sql_query = f"with selected_accounts as (select accountID, count(*) from {finance_db_name}.{table_name_pvc} group by accountID having count(*)>3 union all select accountID, count(*) from {finance_db_name}.{table_name_pubsec} group by accountID having count(*)>3)\
select \
  month, \
  year, \
  accountID, \
  sum(DOLLAR_DBU) as DOLLAR_DBU \
from \
  (select month, year, accountID, dbuDollars as DOLLAR_DBU from {finance_db_name}.{table_name_pvc} where accountID in (select accountID from selected_accounts) union all select month, year, accountID, dbuDollars as DOLLAR_DBU from {finance_db_name}.{table_name_pubsec} where accountID in (select accountID from selected_accounts) ) \
group by \
  month, \
  year, \
  accountID"

# COMMAND ----------

# autoamte date generation

from datetime import date
from datetime import timedelta
from pandas.tseries.offsets import MonthEnd
from pyspark.sql import SparkSession
import yaml

# model etting
hyperparameter_budget = 1200

# accounts
accountIDs = list(spark.sql(f"select concat(\"'\",accountID,\"'\") as accountID from {finance_db_name}.{table_name_pvc} group by accountID having count(*)>12 union all select concat(\"'\",accountID,\"'\") as accountID from {finance_db_name}.{table_name_pubsec} group by accountID having count(*)>12").toPandas()["accountID"])

# COMMAND ----------

from pyspark.sql.functions import sequence, to_date, explode, col, expr
import logging 
import math
import numpy as np
from collections import defaultdict
import warnings
import pandas as pd
from greykite.framework.templates.autogen.forecast_config import ForecastConfig
from greykite.framework.templates.autogen.forecast_config import MetadataParam
from greykite.framework.templates.forecaster import Forecaster
from greykite.framework.templates.model_templates import ModelTemplateEnum
from greykite.framework.utils.result_summary import summarize_grid_search_results
from greykite.framework.templates.autogen.forecast_config import ModelComponentsParam
from greykite.framework.templates.autogen.forecast_config import ComputationParam
from greykite.framework.templates.autogen.forecast_config import EvaluationPeriodParam
from greykite.common.evaluation import EvaluationMetricEnum
from greykite.framework.templates.autogen.forecast_config import EvaluationMetricParam
import greykite.common.constants as cst
from greykite.common.features.timeseries_features import get_logistic_func


logistic_func = get_logistic_func(
    growth_rate=0.5,        # how fast the values go from floor to capacity
    capacity=2000.0,        # in units of the timeseries value
    floor=0.0,              # in units of the timeseries value
    inflection_point=1.0)   # in units of continuous_time_col. How far after the changepoint to place the inflection point

# turn off the warnings
logging.getLogger("py4j").setLevel(logging.ERROR)
warnings.filterwarnings("ignore")


def get_data(accountID, start_date, end_date):

  '''get latest data from the table for training and testing'''
  spark.sql(f"drop table if exists ai.pvc_account_pvc")
  spark.sql(f"create table ai.pvc_account_pvc as ({sql_query})")
  data = spark.sql(f"select last_day(cast(make_date(year, month, 1) as date)) as date, DOLLAR_DBU as dbu_dollar from ai.pvc_account_pvc where accountID = {accountID} and cast(make_date(year, month, 1) as date) between {start_date} and {end_date} \
    union all \
    select last_day(date) as date,dbu_dollar from pvc_future_data")
  return data



def make_future_date(start_date, end_date):
  '''create dummy data for future predictions'''
  future_data = spark.sql(f"SELECT sequence(to_date({start_date}), to_date({end_date}), interval 1 month) as date, 0 as dbu_dollar").withColumn("date", explode(col("date")))
  future_data.createOrReplaceTempView("pvc_future_data")
  return future_data


def prepare_data(data, test_start_date):
  '''prepare the data for training. It created different regressors'''
  df = data.toPandas()
  df['date'] = pd.to_datetime(df['date'])
  df['month_of_year'] = df['date'].dt.month_name().astype(str)
  df["quarter"] = df['date'].dt.quarter.astype(str)
  df['dbu_dollar'] = df['dbu_dollar'].astype(float)
  df = pd.get_dummies(df)
  df.loc[df['date']>=test_start_date,'dbu_dollar']=np.nan
  return df

  
def make_growth_param():
  '''create param growth'''
  growth = dict(growth_term=["linear", "quadratic", "sqrt"])
  return growth

def make_seasonality():
    seasonality = dict(
      yearly_seasonality=[True, False, "auto"],
      quarterly_seasonality=[True, False, "auto"],
      monthly_seasonality= False,
      weekly_seasonality=False,
      daily_seasonality= False
      )
    return seasonality
  
def make_events():
  events = dict(
  daily_event_df_dict = {
                "custom_event": pd.DataFrame({
                    "date": ["2019-01-01",
                             "2019-04-01",
                             "2019-07-01",
                             "2019-10-01",
                             "2020-01-01",
                             "2020-04-01",
                             "2020-07-01",
                             "2020-10-01",
                             "2021-01-01",
                             "2021-04-01",
                             "2021-07-01",
                             "2021-10-01",
                             "2019-02-01", 
                             "2019-05-01", 
                             "2019-05-24", 
                             "2019-08-01", 
                             "2019-11-01", 
                             "2020-02-01",
                             "2020-05-01", 
                             "2020-07-22", 
                             "2020-08-01", 
                             "2020-11-01", 
                             "2021-02-01",
                             "2021-05-01",
                             "2021-06-24",
                             "2021-08-01", 
                             "2021-11-01",
                            "2019-02-05",
                            "2019-10-22",
                            "2021-02-01"
                            ],
                  
                    "event_name": ["q1",
                                   "q2",
                                   "q3",
                                   "q4",
                                   "q1",
                                   "q2",
                                   "q3",
                                   "q4",
                                   "q1",
                                   "q2",
                                   "q3",
                                   "q4",
                                   "fyq1", 
                                   "fyq2", 
                                   "summit", 
                                   "fyq3", 
                                   "fyq4",
                                   "fyq1", 
                                   "fyq2", 
                                   "summit", 
                                   "fyq3", 
                                   "fyq4", 
                                   "fyq1", 
                                   "fyq2", 
                                   "summit", 
                                   "fyq3", 
                                   "fyq4",
                                  "funding_1",
                                  "funding_2",
                                  "funding_3"
                                  ]
                })
            }
     )
  return events

def make_lags_param():
  '''create lags param'''
  lag_dict = dict(orders=[1, 12, 24])
  orders_list = [[2, 3]]
  agg_lag_dict = dict(orders_list=orders_list)
  autoreg_dict = dict(lag_dict=lag_dict, agg_lag_dict=agg_lag_dict)
  return autoreg_dict

def make_changepoint_param():
  changepoints=dict(
    changepoints_dict=[dict(
      method="auto",
      regularization_strength=0.8,
      resample_freq="30D",
      actual_changepoint_min_distance="30D",
      potential_changepoint_distance="30D",
      no_changepoint_proportion_from_end=0.1,
      yearly_seasonality_order=4,
      combine_changepoint_min_distance="60D", 
      keep_detected=False
    ),
      dict(method="auto",
      regularization_strength=0.5,
      resample_freq="60D",
      actual_changepoint_min_distance="60D",
      potential_changepoint_distance="30D",
      no_changepoint_proportion_from_end=0.01,
      yearly_seasonality_order=7,
      combine_changepoint_min_distance="60D", 
      keep_detected=True),
                       
       dict(method="auto",
      regularization_strength=0.2,
      resample_freq="90D",
      actual_changepoint_min_distance="30D",
      potential_changepoint_distance="60D",
      no_changepoint_proportion_from_end=0.05,
      yearly_seasonality_order=3,
      combine_changepoint_min_distance="90D", 
      keep_detected=False),
                       dict(
      method="auto",
      regularization_strength=0.4,
      resample_freq="120D",
      actual_changepoint_min_distance="60D",
      potential_changepoint_distance="30D",
      no_changepoint_proportion_from_end=0.1,
      yearly_seasonality_order=6,
      combine_changepoint_min_distance="90D", 
      keep_detected=True
    ),
      dict(method="auto",
      regularization_strength=0.6,
      resample_freq="30D",
      actual_changepoint_min_distance="120D",
      potential_changepoint_distance="60D",
      no_changepoint_proportion_from_end=0.1,
      yearly_seasonality_order=10,
      combine_changepoint_min_distance="30D", 
      keep_detected=False),
                       
       dict(method="auto",
      regularization_strength=0.9,
      resample_freq="60D",
      actual_changepoint_min_distance="90D",
      potential_changepoint_distance="30D",
      no_changepoint_proportion_from_end=0.1,
      yearly_seasonality_order=9,
      combine_changepoint_min_distance="30D", 
      keep_detected=False),
     dict(
        method="uniform",
        n_changepoints=3),
                          dict(
        method="uniform",
        n_changepoints=5),
                          dict(
        method="uniform",
        n_changepoints=8),
                          dict(
        method="uniform",
        n_changepoints=12),
                          dict(
        method="uniform",
        n_changepoints=15),
                          dict(
        method="uniform",
        n_changepoints=18),
                          dict(
        method="uniform",
        n_changepoints=20),
                          dict(
        method="uniform",
        n_changepoints=25),
                          dict(
        method="uniform",
        n_changepoints=30),   
                          dict(
        method="uniform",
        n_changepoints=35),
                       dict(
        method="uniform",
        n_changepoints=40)
      
     ],
    seasonality_changepoints_dict=[
        dict()
    ]
  
    )
  return changepoints





def make_computation_param(hyperparameter_budget):
  computation = ComputationParam(
     hyperparameter_budget= hyperparameter_budget,
     n_jobs = -1
  )
  return computation

def make_evaluation_metric(cv_selection_metric = EvaluationMetricEnum.SymmetricMeanAbsolutePercentError.name, 
                             cv_report_metrics =[EvaluationMetricEnum.RootMeanSquaredError.name,
                                                 EvaluationMetricEnum.MeanAbsoluteError.name,
                                                 EvaluationMetricEnum.MeanAbsolutePercentError.name,
                                                EvaluationMetricEnum.SymmetricMeanAbsolutePercentError.name],
                             agg_periods = 1,
                             agg_func = np.sum,
                             relative_error_tolerance = 0.05):
  '''create evaluation param'''
  evaluation_metric = EvaluationMetricParam(cv_selection_metric=cv_selection_metric,
                                            cv_report_metrics= cv_report_metrics,
                                            agg_periods=agg_periods,
                                            agg_func=agg_func,
                                            relative_error_tolerance=relative_error_tolerance)
  return evaluation_metric


def make_evaluation_period(test_horizon=0,
                             cv_horizon=3,
                             cv_expanding_window=True,
                             cv_use_most_recent_splits=True,
                             cv_periods_between_splits=3,
                             cv_periods_between_train_test=0,
                             cv_max_splits=4,
                             cv_min_train_periods=None):
  
  
  evaluation_period = EvaluationPeriodParam(
    test_horizon=test_horizon,
    cv_horizon=cv_horizon,
    cv_min_train_periods=cv_min_train_periods,
    cv_expanding_window=cv_expanding_window,
    cv_use_most_recent_splits=cv_use_most_recent_splits,
    cv_periods_between_splits=cv_periods_between_splits,
    cv_periods_between_train_test=cv_periods_between_train_test,
    cv_max_splits=cv_max_splits
 )
  return evaluation_period
  
  
def make_model_components(growth=None,seasonality=None, 
                      events=None,autoregression=None, 
                      changepoints=None,regressors=None,
                      uncertainty=None, custom=None, 
                      hyperparameter_override=None ):
  
  '''create model component'''
  model_components = ModelComponentsParam(
    growth=growth,
    seasonality=seasonality,
    events=events,
    autoregression = autoregression,
    changepoints=changepoints,
    regressors=regressors,
    uncertainty=uncertainty,
    custom=custom,
    hyperparameter_override=hyperparameter_override
    )
  return model_components

def get_grid_search_result(result):
  grid_search = result.grid_search
  cv_results = summarize_grid_search_results(
    grid_search=grid_search,
    decimals=1,
    cv_report_metrics=None,
    column_order=["rank", "mean_test", "split_test", "mean_train", "split_train", "mean_fit_time", "mean_score_time", "params"])
  # Transposes to save space in the printed output
  cv_results["params"] = cv_results["params"].astype(str)
  cv_results.set_index("params", drop=True, inplace=True)
  return cv_results.transpose()

def get_backtest_result(result):
  backtest = result.backtest
  backtest_eval = defaultdict(list)
  for metric, value in backtest.train_evaluation.items():
      backtest_eval[metric].append(value)
      backtest_eval[metric].append(backtest.test_evaluation[metric])
  metrics = pd.DataFrame(backtest_eval, index=["train", "test"]).T
  return metrics

def forecast_future(result, horizon):
  model = result.model
  future_df = result.timeseries.make_future_dataframe(
    periods=horizon,
    include_history=False)
  forecast = model.predict(future_df)[["ts","forecast", "forecast_lower", "forecast_upper"]]
  return forecast

# COMMAND ----------

def run_forecasting(accountID, start_date, end_date, backtest_date, hyperparameter_budget):

  metadata = MetadataParam(
      time_col="date",  
      value_col="dbu_dollar",  
      freq="M",
      anomaly_info =None)

  data = get_data(accountID, start_date, end_date)
  df = prepare_data(data, backtest_date)
  growth_param = make_growth_param()
  seasoanlity_param= make_seasonality()
  #events_param = make_events()
  lags_param = make_lags_param()
  changepoint_param = make_changepoint_param()
  evaluation_period = make_evaluation_period()
  computation_param = make_computation_param(hyperparameter_budget)
  evaluation_metric = make_evaluation_metric(cv_selection_metric = EvaluationMetricEnum.SymmetricMeanAbsolutePercentError.name, 
                               cv_report_metrics =[EvaluationMetricEnum.RootMeanSquaredError.name,
                                                   EvaluationMetricEnum.MeanAbsoluteError.name,
                                                   EvaluationMetricEnum.MeanAbsolutePercentError.name,
                                                  EvaluationMetricEnum.SymmetricMeanAbsolutePercentError.name],
                               agg_periods = 1,
                               agg_func = np.sum,
                               relative_error_tolerance = 0.05)

  regressors_param=dict(
    regressor_cols=list(df.columns[2:])
  )

  model_components = make_model_components(growth = growth_param,
                                           seasonality = seasoanlity_param,
                                           #events = events_param,
                                           changepoints = changepoint_param, 
                                           regressors = regressors_param,
                                           custom = dict(
    fit_algorithm_dict=dict(
        fit_algorithm="ridge",
        fit_algorithm_params={
            "normalize": True
        }
    )
)
                      )
  forecaster = Forecaster()  
  result = forecaster.run_forecast_config( 
      df=df,
      config=ForecastConfig(
          model_template=ModelTemplateEnum.SILVERKITE.name,
          forecast_horizon= horizon,  
          coverage=0.95,   
          metadata_param=metadata,
          model_components_param = model_components,
          evaluation_period_param = evaluation_period,
          evaluation_metric_param = evaluation_metric,
          computation_param=computation_param
      )
  )
  return result

# COMMAND ----------

# scoring
from pyspark.sql.functions import lit, current_timestamp

class prep_results ():
  
  def __init__(self, accountID, start_date, end_date, backtest_date, hyperparameter_budget, backtest_quarter):
    self.accountID =  accountID
    self.start_date = start_date
    self.end_date = end_date
    self.backtest_date = backtest_date 
    self.hyperparameter_budget = hyperparameter_budget
    self.backtest_quarter = backtest_quarter
    
  def run(self, output_folder):
    print(f"Starting {self.accountID}-{self.start_date}-{self.end_date}-{self.backtest_date}-{self.hyperparameter_budget}")
    result = run_forecasting(self.accountID, self.start_date, self.end_date, self.backtest_date, self.hyperparameter_budget)
    grid_search = (pd.DataFrame(get_grid_search_result(result)).reset_index())
    grid_search = pd.DataFrame(np.vstack([grid_search.columns, grid_search])).astype(str)
    
    spark_df_forecast = (spark.createDataFrame(forecast_future(result, horizon))
    .withColumn("accountID",lit(self.accountID))
    .withColumn("backtest_quarter",lit(self.backtest_quarter))
    .withColumn("timestamp", current_timestamp()))
    
    print(f"We are writing {self.accountID}, {self.backtest_quarter} into  table")
    (spark_df_forecast.write.format("delta")
    .mode("append")
    .saveAsTable(f"ai.{output_folder}"))
    
    print(f"Process done for {self.accountID}, {self.backtest_quarter} \n")

# COMMAND ----------

import datetime

i = 1
# on the fly setting.
# TODO: we might need to move some of these configs to the yaml file 

if not backtest:
  output_folder ='account_pvc'
  spark.sql(f"drop table if exists ai.{output_folder}")
else:
  output_folder = output_folder
  spark.sql(f"drop table if exists ai.{output_folder}")

for accountID in accountIDs:
  print(accountID)
  print(f"building {i} model out of {len(accountIDs)}")
  pandas_df = spark.sql(f"select distinct(make_date(year, month, 1)) as date from (select year, month from {finance_db_name}.{table_name_pvc} where accountID = {accountID} union all select year, month from {finance_db_name}.{table_name_pubsec} where accountID = {accountID})").toPandas()
  max_date = pandas_df.max()[0]
  if pandas_df.max()[0]<datetime.date(2022, 1, 1):
    print("Not enough recent data")
    i = i + 1
    continue
  start_date = "'"+str(pandas_df.min()[0])+"'"
  start_date_future = (max_date + MonthEnd(2)).strftime('%Y-%m-%d')
  start_date_future_string = "'"+str(start_date_future)+"'"
  end_date_future = (max_date + timedelta(days= (horizon-1) * 30) + MonthEnd(3)).strftime('%Y-%m-%d')
  end_date_future_string = "'"+str(end_date_future)+"'"
  end_date_string = "'"+str(max_date)+"'"
  today = date.today()
  forecast_as_of_str = str(today)
  end_date = end_date_string
  backtest_quarter = "'none'"
  backtest_date = start_date_future_string
  make_future_date(start_date_future_string, end_date_future_string)
  prep_results(accountID, start_date, end_date_string, backtest_date, hyperparameter_budget, backtest_quarter).run(output_folder)
  i = i + 1

# COMMAND ----------

from pyspark.sql.functions import month, year, dayofmonth, date_trunc, lit
as_of_date = spark.sql(f"select max(weekStart)+7 as as_of_date from usage_forecasting_acct.account_week_master").first()[0]

def make_future_date_daily(start_date, end_date):
  '''create dummy data for future predictions'''
  future_data = spark.sql(f"SELECT sequence(to_date({start_date}), to_date({end_date}), interval 1 day) as date").withColumn("date", explode(col("date")))
  return future_data

# find accounts that were forecasted/not-forecasted
all_accounts = spark.sql(f"select distinct accountID from (select accountID from {finance_db_name}.{table_name_pvc} union all select accountID from {finance_db_name}.{table_name_pubsec})")
forecasted_accounts = spark.sql(f"select distinct replace(accountID,\"'\",\"\") as accountID from ai.{output_folder}")
not_forecasted_accounts = all_accounts.subtract(forecasted_accounts).withColumnRenamed("accountID","sfdcAccountId")

# Adjust the forecast to daily for all forecasted accounts                                
calendar = (make_future_date_daily("'2018-01-01'","'2030-01-01'")).withColumn("month",month(col("date"))).withColumn("year",year(col("date")))
forecast = spark.sql(f"select replace(accountID,\"'\",\"\") as sfdcAccountId,cast(ts as date) as date, forecast, '{str(as_of_date)}' as train_date from ai.{output_folder}") .withColumn("month",month(col("date"))).withColumn("year",year(col("date"))).withColumn("num_day_month",dayofmonth(col("date")))       
forecast_daily = forecast.alias("forecast").join(calendar.alias("calendar"),(forecast.month == calendar.month) & (forecast.year == calendar.year), "left").select("forecast.sfdcAccountId",date_trunc('week', col("calendar.date")).cast("date").alias('week'),(col("forecast.forecast")/col("forecast.num_day_month")).alias("daily_forecast"),"forecast.train_date").groupBy([col("sfdcAccountId"), col("week"),col("train_date")]).sum().withColumnRenamed("sum(daily_forecast)","forecast").select("sfdcAccountId","forecast","week","train_date")

# Populated 0 for all inactive accounts or those with minimal recent history
not_forecasted_accounts_forecast = not_forecasted_accounts.alias("not_forecasted").crossJoin(forecast_daily.drop_duplicates(["week","train_date"]).alias("forecasted")).select("not_forecasted.sfdcAccountId",lit(0).alias("forecast"), "forecasted.week","forecasted.train_date")                             
 
                                
# Write final tables to db
final_pvc_forecast = forecast_daily.unionAll(not_forecasted_accounts_forecast)
if not backtest:
  final_pvc_forecast.write.mode("append").format("delta").saveAsTable(f"usage_forecasting_prod.{output_folder}_forecast_ai_pipeline")
else:
  final_pvc_forecast.write.mode("overwrite").format("delta").saveAsTable(f"usage_forecasting_prod.{output_folder}_pvc_forecast")
