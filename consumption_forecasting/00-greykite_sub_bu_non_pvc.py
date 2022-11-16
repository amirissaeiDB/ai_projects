# Databricks notebook source
# MAGIC %md
# MAGIC # Greykite

# COMMAND ----------

# MAGIC %fs rm dbfs:/amir/output_table_name.txt

# COMMAND ----------

# autoamte date generation

from datetime import timedelta
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
import yaml

# start the spark session. Need this for DBconnet. Can be removed if you are using DB notebook.
spark = SparkSession.builder.appName('temps-demo').config("spark.driver.bindAddress", "127.0.0.1").getOrCreate()
# Need this for DB Connect. Can be removed if using notebook
# spark.sql("SET spark.sql.sources.default=delta")
# spark.sql("SET spark.sql.legacy.createHiveTableByDefault=false")
# spark.sql("SET spark.sql.hive.convertCTAS=true")

# read the global config file
with open("../greykite/conf_sub_bu.yaml", "r") as stream:
    conf = (yaml.safe_load(stream))


# setting parameters for data
dollar_dbu_non_pvc_database = conf['data']['dollar_dbu_non_pvc_database']
dollar_dbu_non_pvc_table = conf['data']['dollar_dbu_non_pvc_table']
temp_database = conf['data']['temp_database']
temp_table = conf['data']['temp_table']['temp_table_non_pvc']
future_data_database = conf['data']['future_data_database']
future_data_table = conf['data']['future_data_table']

#user-defined curves
curve_ind = conf['curve_ind']
#2021 growth cirves
amer_enterprise_2021 = conf['curves']['2021_growth']['amer_enterprise']
amer_gsc_2021 = conf['curves']['2021_growth']['amer_gsc']
amer_regulated_verticals_2021 = conf['curves']['2021_growth']['amer_regulated_verticals']
digital_native_2021 = conf['curves']['2021_growth']['digital_native']
emea_2021 = conf['curves']['2021_growth']['emea']
apj_2021 = conf['curves']['2021_growth']['apj']
microsoft_house_account_2021 = conf['curves']['2021_growth']['microsoft_house_account']

#2020 growth curves
amer_enterprise_2020 = conf['curves']['2020_growth']['amer_enterprise']
amer_gsc_2020 = conf['curves']['2020_growth']['amer_gsc']
amer_regulated_verticals_2020 = conf['curves']['2020_growth']['amer_regulated_verticals']
digital_native_2020 = conf['curves']['2020_growth']['digital_native']
emea_2020 = conf['curves']['2020_growth']['emea']
apj_2020 = conf['curves']['2020_growth']['apj']
microsoft_house_account_2020 = conf['curves']['2020_growth']['microsoft_house_account']

# 2019 growth curves
amer_enterprise_2019 = conf['curves']['2019_growth']['amer_enterprise']
amer_gsc_2019 = conf['curves']['2019_growth']['amer_gsc']
amer_regulated_verticals_2019 = conf['curves']['2019_growth']['amer_regulated_verticals']
digital_native_2019 = conf['curves']['2019_growth']['digital_native']
emea_2019 = conf['curves']['2019_growth']['emea']
apj_2019 = conf['curves']['2019_growth']['apj']
microsoft_house_account_2019 = conf['curves']['2019_growth']['microsoft_house_account']

#2018 growth curves
amer_enterprise_2018 = conf['curves']['2018_growth']['amer_enterprise']
amer_gsc_2018 = conf['curves']['2018_growth']['amer_gsc']
amer_regulated_verticals_2018 = conf['curves']['2018_growth']['amer_regulated_verticals']
digital_native_2018 = conf['curves']['2018_growth']['digital_native']
emea_2018 = conf['curves']['2018_growth']['emea']
apj_2018 = conf['curves']['2018_growth']['apj']
microsoft_house_account_2018 = conf['curves']['2018_growth']['microsoft_house_account']




# busines sunits
#dbutils = DBUtils(spark)
#dbutils.widgets.get("bu") #
bus =conf['sub_bu']['non_pvc']


# setting parameters for model
horizon = conf['model']['non_pvc']['horizon']
hyperparameter_budget = conf['model']['non_pvc']['hyperparameter_budget']

# setting parametrs for run
start_date = conf['run']['start_date']


# need the maximum data to create forecast_as_of on the fly
maximum_date = spark.sql(f"select max(date) as maximum_date \
               from {dollar_dbu_non_pvc_database}.{dollar_dbu_non_pvc_table}")\
              .toPandas()["maximum_date"][0]
forecast_as_of = maximum_date + timedelta(days=1)
forecast_as_of_str = str(forecast_as_of)

# this table i used for the final output. It is a temporary table.
# TODO: clean up the tables at the end. This is not being done at the moment. 
output_table_name = forecast_as_of_str.replace("-","_")


spark.sql(f"drop table if exists {temp_database}.{output_table_name}")
spark.sql(f"drop table if exists {temp_database}.{temp_table}")

# This query is used to create trned curves for each BUs. The process should be automated on the fly. 
spark.sql(f"create table {temp_database}.{temp_table} as \
  (select date, bu, sum(dbuDollars) as dollar_dbu from {dollar_dbu_non_pvc_database}.{dollar_dbu_non_pvc_table}\
  where date<= '{maximum_date}' group by date, bu)")

# COMMAND ----------

from pyspark.sql.functions import sequence, to_date, explode, col, expr, lag, mean, year, month, sum, lit, add_months
from pyspark.sql.window import Window
import logging 
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


# turn off the warnings
logging.getLogger("py4j").setLevel(logging.ERROR)
warnings.filterwarnings("ignore")

def make_future_date(start_date, end_date):
  '''create dummy data for future predictions'''
  future_data = spark.sql(f"SELECT sequence(to_date({start_date}), to_date({end_date}), interval 1 day) as date, 0 as dollar_dbu")\
    .withColumn("date", explode(col("date")))
  future_data.createOrReplaceTempView(f"{future_data_table}")
  return future_data

def get_data(bu, start_date, end_date):
  '''get latest dat from the table for training and testing'''

  if curve_ind != 0:
    data = spark.sql(f"select date, dollar_dbu,\
      case when {bu} = 'AMER Enterprise' then {amer_enterprise_2021} \
      when {bu} = 'AMER GSC' then {amer_gsc_2021} \
      when {bu} = 'AMER Regulated Verticals' then {amer_regulated_verticals_2021} \
      when {bu} = 'Digital Native' then {digital_native_2021} \
      when {bu} = 'EMEA' then {emea_2021}\
      when {bu} = 'APJ' then {apj_2021} \
      when {bu} = 'Microsoft House Account' then {microsoft_house_account_2021} end as growth_2021, \
      case when {bu} = 'AMER Enterprise' then {amer_enterprise_2020} \
      when {bu} = 'AMER GSC' then {amer_gsc_2020} \
      when {bu} = 'AMER Regulated Verticals' then {amer_regulated_verticals_2020} \
      when {bu} = 'Digital Native' then {digital_native_2020} \
      when {bu} = 'EMEA' then {emea_2020}\
      when {bu} = 'APJ' then {apj_2020} \
      when {bu} = 'Microsoft House Account' then {microsoft_house_account_2020} end as growth_2020, \
      case when {bu} = 'AMER Enterprise' then {amer_enterprise_2019} \
      when {bu} = 'AMER GSC' then {amer_gsc_2019} \
      when {bu} = 'AMER Regulated Verticals' then {amer_regulated_verticals_2019} \
      when {bu} = 'Digital Native' then {digital_native_2019} \
      when {bu} = 'EMEA' then {emea_2019}\
      when {bu} = 'APJ' then {apj_2019} \
      when {bu} = 'Microsoft House Account' then {microsoft_house_account_2019} end as growth_2019, \
      case when {bu} = 'AMER Enterprise' then {amer_enterprise_2018} \
      when {bu} = 'AMER GSC' then {amer_gsc_2018} \
      when {bu} = 'AMER Regulated Verticals' then {amer_regulated_verticals_2018} \
      when {bu} = 'Digital Native' then {digital_native_2018} \
      when {bu} = 'EMEA' then {emea_2019} \
      when {bu} = 'APJ' then {apj_2019} \
      when {bu} = 'Microsoft House Account' then {microsoft_house_account_2018} end as growth_2018 \
      from (\
        select date, row_number() over(order by date asc) as rn, dollar_dbu \
        from (\
          select date, dollar_dbu from {temp_database}.{temp_table} where bu = {bu} and date>={start_date} and date<={end_date} \
          union all \
          select * from future_data \
            )\
          )")
  else:
    data = spark.sql(f"select date, dollar_dbu\
        from (\
          select date, dollar_dbu from {temp_database}.{temp_table} where bu = {bu} and date>={start_date} and date<={end_date} \
          union all \
          select * from future_data \
            )\
          ")
  return data


def prepare_data(data, test_start_date):
  '''prepare the data for training. It created different regressors'''
  df = data.toPandas()
  df['date'] = pd.to_datetime(df['date'])
  df['day_of_week'] = df['date'].dt.day_name().astype(str)
  df['month_of_year'] = df['date'].dt.month_name().astype(str)
  df["is_quarter_start"] = df['date'].dt.is_quarter_start.astype(str)
  df["is_quarter_end"] = df['date'].dt.is_quarter_end.astype(str)
  df["is_month_end"] = df['date'].dt.is_month_end.astype(str)
  df["is_month_start"] = df['date'].dt.is_month_start.astype(str)
  df["quarter"] = df['date'].dt.quarter.astype(str)
  df["weekofyear"] = df['date'].dt.weekofyear.astype(str)
  df = pd.get_dummies(df)
  df.loc[df['date']>=test_start_date,'dollar_dbu']=np.nan
  return df


def make_anamaly_param():
  '''used to create anamaly pram for the model'''
  anomaly_df = pd.DataFrame(
    {
    cst.START_DATE_COL: ["22/3/2020"],
    cst.END_DATE_COL: ["13/12/2020"],
    cst.ADJUSTMENT_DELTA_COL: [200000]
     })

  anomaly_info = [
    {
        "value_col": "dollar_dbu",
        "anomaly_df": anomaly_df,
        "adjustment_delta_col": cst.ADJUSTMENT_DELTA_COL,
    }
      ]
  return anomaly_info
  
def make_growth_param():
  '''create param growth'''
  growth = dict(growth_term=["linear", "quadratic", "sqrt"])
  return growth

def make_seasonality():
    seasonality = dict(
      yearly_seasonality=["auto"],
      quarterly_seasonality=["auto"],
      monthly_seasonality=["auto"],
      weekly_seasonality=["auto"],
      daily_seasonality= False
      )
    return seasonality
  
def make_events():
  events = dict(
  holidays_to_model_separately = ["auto",
        None],
  holiday_lookup_countries = ["auto"],
  holiday_pre_num_days=[2],
  holiday_post_num_days=[2],
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
                             "2020-06-23",
                             "2020-08-01", 
                             "2020-11-01", 
                             "2021-02-01",
                             "2021-05-01",
                             "2021-06-24",
                             "2021-08-01", 
                             "2021-11-01",
                            "2019-02-05",
                            "2019-10-22"                            
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
                                  "funding_2"                                  
                                  ]
                })
            }
           
     )
  return events

def make_lags_param():
  '''create lags param'''
  lag_dict = dict(orders=[1, 7])
  orders_list = [[1, 2, 3, 4, 5, 6, 7]]
  agg_lag_dict = dict(orders_list=orders_list)
  autoreg_dict = dict(lag_dict=lag_dict, agg_lag_dict=agg_lag_dict)
  return autoreg_dict

def make_changepoint_param():
  changepoints=dict(
    changepoints_dict=[dict(
      method="auto",
      regularization_strength=0.8,
      resample_freq="7D",
      actual_changepoint_min_distance="30D",
      potential_changepoint_distance="10D",
      no_changepoint_proportion_from_end=0.3,
      yearly_seasonality_order=4,
      dates=["2021-03-01"],
      combine_changepoint_min_distance="30D", 
      keep_detected=False
    ),
      dict(method="auto",
      regularization_strength=0.5,
      resample_freq="10D",
      actual_changepoint_min_distance="60D",
      potential_changepoint_distance="20D",
      no_changepoint_proportion_from_end=0.3,
      yearly_seasonality_order=7,
      dates=["2021-03-01"],
      combine_changepoint_min_distance="40D", 
      keep_detected=True),
                       
       dict(method="auto",
      regularization_strength=0.2,
      resample_freq="20D",
      actual_changepoint_min_distance="10D",
      potential_changepoint_distance="5D",
      no_changepoint_proportion_from_end=0.2,
      yearly_seasonality_order=3,
      dates=["2021-03-01"],
      combine_changepoint_min_distance="50D", 
      keep_detected=False),
                       dict(
      method="auto",
      regularization_strength=0.4,
      resample_freq="30D",
      actual_changepoint_min_distance="20D",
      potential_changepoint_distance="10D",
      no_changepoint_proportion_from_end=0.05,
      yearly_seasonality_order=6,
      dates=["2021-03-01"],
      combine_changepoint_min_distance="30D", 
      keep_detected=True
    ),
      dict(method="auto",
      regularization_strength=0.6,
      resample_freq="40D",
      actual_changepoint_min_distance="40D",
      potential_changepoint_distance="7D",
      no_changepoint_proportion_from_end=0.1,
      yearly_seasonality_order=10,
      dates=["2021-03-01"],
      combine_changepoint_min_distance="10D", 
      keep_detected=False),
                       
       dict(method="auto",
      regularization_strength=0.9,
      resample_freq="60D",
      actual_changepoint_min_distance="50D",
      potential_changepoint_distance="D",
      no_changepoint_proportion_from_end=0.01,
      yearly_seasonality_order=9,
      dates=["2021-03-01"],
      combine_changepoint_min_distance="20D", 
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
        dict(),  
        dict(
            regularization_strength=0.5,
            seasonality_components_df=pd.DataFrame({
                "name": ["tod", "tow", "conti_year"],
                "period": [24.0, 7.0, 1.0],
                "order": [3, 3, 5],
                "seas_names": ["daily", "weekly", "yearly"]}),
          resample_freq="7D",
               actual_changepoint_min_distance="30D",
           potential_changepoint_distance="10D",
           no_changepoint_proportion_from_end=0.3
      
      

        ),
      dict(
            regularization_strength=0.8,
        seasonality_components_df=pd.DataFrame({
                "name": ["tod", "tow", "conti_year"],
                "period": [24.0, 7.0, 1.0],
                "order": [3, 3, 5],
                "seas_names": ["daily", "weekly", "yearly"]}),
          resample_freq="30D",
               actual_changepoint_min_distance="60D",
           potential_changepoint_distance="20D",
           no_changepoint_proportion_from_end=0.2
        ),
      dict(
            regularization_strength=0.1,
        seasonality_components_df=pd.DataFrame({
                "name": ["tod", "tow", "conti_year"],
                "period": [24.0, 7.0, 1.0],
                "order": [3, 3, 5],
                "seas_names": ["daily", "weekly", "yearly"]}),
          resample_freq="30D",
               actual_changepoint_min_distance="90D",
           potential_changepoint_distance="10D",
           no_changepoint_proportion_from_end=0.3
        )
    ]
  
    )
  return changepoints

def make_computation_param(hyperparameter_budget):
  computation = ComputationParam(
     hyperparameter_budget= hyperparameter_budget,
     n_jobs = -1
  )
  return computation

def make_evaluation_metric(cv_selection_metric = EvaluationMetricEnum.MeanAbsolutePercentError.name, 
                             cv_report_metrics =[EvaluationMetricEnum.RootMeanSquaredError.name,
                                                 EvaluationMetricEnum.MeanAbsoluteError.name,
                                                 EvaluationMetricEnum.MeanAbsolutePercentError.name],
                             agg_periods = 30,
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
                             cv_horizon=horizon,
                             cv_expanding_window=True,
                             cv_use_most_recent_splits=True,
                             cv_periods_between_splits=horizon,
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

def run_forecasting(bu, start_date, end_date, backtest_date, hyperparameter_budget):

  metadata = MetadataParam(
      time_col="date",  
      value_col="dollar_dbu",  
      freq="D",
      anomaly_info =None)

  data = get_data(bu, start_date, end_date)
  df = prepare_data(data, backtest_date)
  anamaly_param = make_anamaly_param()
  growth_param = make_growth_param()
  seasoanlity_param= make_seasonality()
  events_param = make_events()
  lags_param = make_lags_param()
  changepoint_param = make_changepoint_param()
  evaluation_period = make_evaluation_period()
  computation_param = make_computation_param(hyperparameter_budget)
  evaluation_metric = make_evaluation_metric(cv_selection_metric = EvaluationMetricEnum.MeanAbsolutePercentError.name, 
                               cv_report_metrics =[EvaluationMetricEnum.RootMeanSquaredError.name,
                                                   EvaluationMetricEnum.MeanAbsoluteError.name,
                                                   EvaluationMetricEnum.MeanAbsolutePercentError.name],
                               agg_periods = 30,
                               agg_func = np.sum,
                               relative_error_tolerance = 0.05)

  regressors_param=dict(
    regressor_cols=list(df.columns[2:])
  )

  model_components = make_model_components(growth = growth_param,
                                           seasonality = seasoanlity_param,
                                           events = events_param,
                                           changepoints = changepoint_param,
                                           autoregression = None, #dict(autoreg_dict = lags_param),
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

# added for for-loop
from pyspark.sql.functions import lit, current_timestamp

class prep_results ():
  
  def __init__(self, bu, start_date, end_date, backtest_date, hyperparameter_budget, backtest_quarter):
    self.bu =  bu
    self.start_date = start_date
    self.end_date = end_date
    self.backtest_date = backtest_date 
    self.hyperparameter_budget = hyperparameter_budget
    self.backtest_quarter = backtest_quarter
    
  def run(self):
    print(f"Starting {self.bu}-{self.start_date}-{self.end_date}-{self.backtest_date}-{self.hyperparameter_budget}")
    result = run_forecasting(self.bu, self.start_date, self.end_date, self.backtest_date, self.hyperparameter_budget)
    grid_search = (pd.DataFrame(get_grid_search_result(result)).reset_index())
    grid_search = pd.DataFrame(np.vstack([grid_search.columns, grid_search])).astype(str)
  
    spark_df_forecast = (spark.createDataFrame(forecast_future(result, horizon))
    .withColumn("bu",lit(self.bu))
    .withColumn("backtest_quarter",lit(self.backtest_quarter))
    .withColumn("timestamp", current_timestamp()))
    
    print(f"We are writing {self.bu}, {self.backtest_quarter} into forecast table")
    print(f"Process done for {self.bu}, {self.backtest_quarter} \n")
    (spark_df_forecast.write.format("delta")
    .mode("append")
    .saveAsTable(f"{temp_database}.{output_table_name}"))

# COMMAND ----------

start_date_future = maximum_date + timedelta(days=1)
start_date_future_string = "'"+str(start_date_future)+"'"
end_date_future = maximum_date + timedelta(days=horizon)
end_date_future_stirng = "'"+str(end_date_future)+"'"

make_future_date(start_date_future_string, end_date_future_stirng)

# prediction
hyperparameter_budget = hyperparameter_budget
backtest_quarter = 'none'
end_date = "'"+ str(maximum_date) + "'"
backtest_date = start_date_future_string


# run the forecast  
for bu in bus:
  prep_results(bu, start_date, end_date, backtest_date, hyperparameter_budget, backtest_quarter).run()


if spark.sql(f"select count(*) from {temp_database}.{output_table_name}").first()[0] == len(bus)*horizon:
  print("Greykite run is completed successfully.")
  file = open("/dbfs/amir/output_table_name.txt", "w")
  file.write(output_table_name)
  file.close()
else:
  raise ValueError('Something went wrong. Check the run again.')
