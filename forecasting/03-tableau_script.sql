-- Databricks notebook source


-- COMMAND ----------

-- DBTITLE 1,Silver - Total qtr by forecast_date - Identify new Non-PVC forecast runs
-- MAGIC %python
-- MAGIC forecast_timestamp_mapping = spark.sql("""
-- MAGIC select 
-- MAGIC     a.forecast_as_of,
-- MAGIC     a.score_run_date as timestamp,
-- MAGIC     CASE
-- MAGIC       WHEN month(date_trunc("month", min(a.forecast_as_of))) = 1 THEN (year(date_trunc("year", min(a.forecast_as_of))) * 100) + 04
-- MAGIC       WHEN month(date_trunc("month", min(a.forecast_as_of))) = 2 THEN ((year(date_trunc("year", min(a.forecast_as_of))) + 1) * 100) + 01
-- MAGIC       WHEN month(date_trunc("month", min(a.forecast_as_of))) = 3 THEN ((year(date_trunc("year", min(a.forecast_as_of))) + 1) * 100) + 01
-- MAGIC       WHEN month(date_trunc("month", min(a.forecast_as_of))) = 4 THEN ((year(date_trunc("year", min(a.forecast_as_of))) + 1) * 100) + 01
-- MAGIC       WHEN month(date_trunc("month", min(a.forecast_as_of))) = 5 THEN ((year(date_trunc("year", min(a.forecast_as_of))) + 1) * 100) + 02
-- MAGIC       WHEN month(date_trunc("month", min(a.forecast_as_of))) = 6 THEN ((year(date_trunc("year", min(a.forecast_as_of))) + 1) * 100) + 02
-- MAGIC       WHEN month(date_trunc("month", min(a.forecast_as_of))) = 7 THEN ((year(date_trunc("year", min(a.forecast_as_of))) + 1) * 100) + 02
-- MAGIC       WHEN month(date_trunc("month", min(a.forecast_as_of))) = 8 THEN ((year(date_trunc("year", min(a.forecast_as_of))) + 1) * 100) + 03
-- MAGIC       WHEN month(date_trunc("month", min(a.forecast_as_of))) = 9 THEN ((year(date_trunc("year", min(a.forecast_as_of))) + 1) * 100) + 03
-- MAGIC       WHEN month(date_trunc("month", min(a.forecast_as_of))) = 10 THEN ((year(date_trunc("year", min(a.forecast_as_of))) + 1) * 100) + 03
-- MAGIC       WHEN month(date_trunc("month", min(a.forecast_as_of))) = 11 THEN ((year(date_trunc("year", min(a.forecast_as_of))) + 1) * 100) + 04
-- MAGIC       WHEN month(date_trunc("month", min(a.forecast_as_of))) = 12 THEN ((year(date_trunc("year", min(a.forecast_as_of))) + 1) * 100) + 04
-- MAGIC     END as fcst_qtr_num
-- MAGIC from
-- MAGIC   usage_forecasting_prod.forecast_results a
-- MAGIC   inner join 
-- MAGIC   (
-- MAGIC     select
-- MAGIC       *
-- MAGIC     from
-- MAGIC       (
-- MAGIC         select
-- MAGIC           distinct forecast_as_of,
-- MAGIC           score_run_date
-- MAGIC         from
-- MAGIC           usage_forecasting_prod.forecast_results
-- MAGIC         where
-- MAGIC           lower(is_active) = "true"
-- MAGIC       ) lat 
-- MAGIC 
-- MAGIC       left anti join 
-- MAGIC 
-- MAGIC       (
-- MAGIC         select
-- MAGIC           forecast_date,
-- MAGIC           data_pull_timestamp
-- MAGIC         from
-- MAGIC           usage_forecasting_tab_silver.frozen_actuals_non_pvc_pub
-- MAGIC         group by
-- MAGIC           forecast_date,
-- MAGIC           data_pull_timestamp
-- MAGIC         order by
-- MAGIC           forecast_date desc,
-- MAGIC           data_pull_timestamp
-- MAGIC       ) curr 
-- MAGIC 
-- MAGIC       on curr.forecast_date = lat.forecast_as_of
-- MAGIC       and curr.data_pull_timestamp = lat.score_run_date
-- MAGIC   ) b
-- MAGIC   on a.forecast_as_of = b.forecast_as_of
-- MAGIC   and a.score_run_date = b.score_run_date
-- MAGIC where 
-- MAGIC   lower(is_active) = "true"
-- MAGIC group by
-- MAGIC   a.forecast_as_of,
-- MAGIC   a.score_run_date
-- MAGIC """)
-- MAGIC 
-- MAGIC # Convert to pandas dataframe to loop over
-- MAGIC forecast_timestamp_mapping_pd = forecast_timestamp_mapping.toPandas()
-- MAGIC forecast_timestamp_mapping_pd['forecast_as_of'] = forecast_timestamp_mapping_pd['forecast_as_of'].astype(str)
-- MAGIC forecast_timestamp_mapping_pd['timestamp'] = forecast_timestamp_mapping_pd['timestamp'].astype(str)
-- MAGIC forecast_timestamp_mapping_pd['fcst_qtr_num'] = forecast_timestamp_mapping_pd['fcst_qtr_num'].astype(int)
-- MAGIC 
-- MAGIC forecast_timestamp_mapping_pd

-- COMMAND ----------

-- DBTITLE 1,Silver - Total qtr by forecast_date - Freeze actuals for new Non-PVC forecast runs by appending the actuals to historic table
-- MAGIC %python
-- MAGIC # If forecast_timestamp_mapping_pd dataset is not empty, we need to freeze actuals for new non-pvc forecast runs
-- MAGIC if len(forecast_timestamp_mapping_pd):
-- MAGIC   # Loop over each of the new non-pvc forecast runs and freeze the actuals
-- MAGIC   for index, row in forecast_timestamp_mapping_pd.iterrows():
-- MAGIC     timestamp_arg = str(row['timestamp'])
-- MAGIC     forecast_as_of_arg = str(row['forecast_as_of'])
-- MAGIC     fcst_qtr_num_arg = int(row['fcst_qtr_num'])
-- MAGIC     print(forecast_as_of_arg, " | ", timestamp_arg, " | ", fcst_qtr_num_arg)
-- MAGIC 
-- MAGIC     query = f"""
-- MAGIC       insert into usage_forecasting_tab_silver.frozen_actuals_non_pvc_pub
-- MAGIC       select
-- MAGIC         'actuals' as time_series_type,
-- MAGIC         'actuals' as time_series_type_detailed,
-- MAGIC         'actuals' as time_series_type_detailed_display,
-- MAGIC         "{forecast_as_of_arg}" as forecast_date,
-- MAGIC         lower(platform) as platform,
-- MAGIC         case
-- MAGIC           when upper(region) LIKE "APJ" then 'APAC'
-- MAGIC           else region
-- MAGIC         end as region,
-- MAGIC         l1segment as segment,
-- MAGIC         concat(lower(type), "_dbus") as sku,
-- MAGIC         date,
-- MAGIC         CAST(null as DOUBLE) as dbus,
-- MAGIC         sum(dbuDollars) as dollar_dbus,
-- MAGIC         "{timestamp_arg}" as data_pull_timestamp,
-- MAGIC         "{fcst_qtr_num_arg}" as fcst_qtr_num
-- MAGIC       from
-- MAGIC         finance.dbu_dollars
-- MAGIC         timestamp as of "{timestamp_arg}"
-- MAGIC       group by
-- MAGIC         platform,
-- MAGIC         region,
-- MAGIC         l1segment,
-- MAGIC         type,
-- MAGIC         date
-- MAGIC       order by
-- MAGIC         date desc
-- MAGIC       """
-- MAGIC     spark.sql(query)
-- MAGIC     spark.sql("select count(*) from usage_forecasting_tab_silver.frozen_actuals_non_pvc_pub").show()

-- COMMAND ----------

optimize usage_forecasting_tab_silver.frozen_actuals_non_pvc_pub
zorder by forecast_date, date, region, segment

-- COMMAND ----------

-- DBTITLE 1,Actuals - Primary Non-PVC
-- create table usage_forecasting_tab_silver.actuals_non_pvc_pub using delta as
insert overwrite usage_forecasting_tab_silver.actuals_non_pvc_pub

-- Get the latest Non-PVC actuals from the frozen dataset
select
  time_series_type,
  time_series_type_detailed,
  time_series_type_detailed_display,
  CAST(null as DATE) as forecast_date,
  platform,
  region,
  segment,
  sku,
  date,
  dbus,
  dollar_dbus
from
  usage_forecasting_tab_silver.frozen_actuals_non_pvc_pub frz
  inner join 
  (
    select 
      max(forecast_date) as forecast_date
    from
      usage_forecasting_tab_silver.frozen_actuals_non_pvc_pub
  ) max
  on frz.forecast_date = max.forecast_date

-- select
--   'actuals' as time_series_type,
--   'actuals' as time_series_type_detailed,
--   'actuals' as time_series_type_detailed_display,
--   CAST(null as TIMESTAMP) as forecast_date,
--   lower(platform) as platform,
--   case
--     when upper(region) LIKE "APJ" then 'APAC'
--     else region
--   end as region,
--   l1segment as segment,
--   concat(lower(type), "_dbus") as sku,
--   date,
--   CAST(null as DOUBLE) as dbus,
--   sum(dbuDollars) as dollar_dbus
-- from
--   finance.dbu_dollars
-- group by
--   platform,
--   region,
--   l1segment,
--   type,
--   date

-- COMMAND ----------

-- DBTITLE 1,Actuals - PubSec
-- create table usage_forecasting_tab_silver.actuals_pubsec using delta as
insert overwrite usage_forecasting_tab_silver.actuals_pubsec

select
  'actuals' as time_series_type,
  'actuals - pvc' as time_series_type_detailed,
  'actuals' as time_series_type_detailed_display,
  CAST(null as TIMESTAMP) as forecast_date,
  lower(platform) as platform,
  case
    when upper(region) LIKE "APJ" then 'APAC'
    else region
  end as region,
  l1segment as segment,
  concat(lower(type), "_dbus") as sku,
  make_date(year, month, '1') as date,
  CAST(null as DOUBLE) as dbus,
  sum(dbuDollars) as dollar_dbus
from
  finance.pubsec_usage
group by
  platform,
  region,
  l1segment,
  type,
  year,
  month

-- COMMAND ----------

-- DBTITLE 1,Actuals - PVC
-- create table usage_forecasting_tab_silver.actuals_pvc using delta as
insert overwrite usage_forecasting_tab_silver.actuals_pvc

select
  'actuals' as time_series_type,
  'actuals - pvc' as time_series_type_detailed,
  'actuals' as time_series_type_detailed_display,
  CAST(null as TIMESTAMP) as forecast_date,
  lower(platform) as platform,
  case
    when upper(region) LIKE "APJ" then 'APAC'
    else region
  end as region,
  l1segment as segment,
  concat(lower(type), "_dbus") as sku,
  make_date(year, month, '1') as date,
  CAST(null as DOUBLE) as dbus,
  sum(dbuDollars) as dollar_dbus
from
    finance.pvc_usage
group by
  platform,
  region,
  l1segment,
  type,
  year,
  month

-- COMMAND ----------

-- DBTITLE 1,Actuals - Combined
-- create table usage_forecasting_tab_silver.actuals_combined using delta as
insert overwrite usage_forecasting_tab_silver.actuals_combined

select
  *
from
  usage_forecasting_tab_silver.actuals_non_pvc_pub

union all

select
  *
from
  usage_forecasting_tab_silver.actuals_pubsec

union all

select
  *
from
  usage_forecasting_tab_silver.actuals_pvc

-- COMMAND ----------

-- DBTITLE 1,Non-PVC forecast
-- create table usage_forecasting_tab_silver.forecast_nonpvc using delta as
insert overwrite usage_forecasting_tab_silver.forecast_nonpvc

select
  'future forecast' as time_series_type,
  'future forecast - non-pvc' as time_series_type_detailed,
  'future forecast' as time_series_type_detailed_display,
  forecast_as_of as forecast_date,
  'No Platform' as platform,
  case
    when lower(ts_key) LIKE "%amer%" then 'AMER'
    when lower(ts_key) LIKE "%apj%" then 'APAC'
    when lower(ts_key) LIKE "%emea%" then 'EMEA'
    when lower(ts_key) LIKE "%latam%" then 'LATAM'
    else 'No Region'
  end as region,
  case
    when trim(lower(ts_key)) LIKE "%enterprise%" then 'Enterprise'
    when trim(lower(ts_key)) LIKE "%mm/comm%" then 'MM/Comm'
    when trim(lower(ts_key)) LIKE "%pubsec%" then 'PubSec'
    when trim(lower(ts_key)) LIKE "%digital native%" then 'Digital Native'
    when trim(lower(ts_key)) LIKE "%all%" then 'All'
    else 'No Segment'
  end as segment,
  'No SKU' as sku,
  date,
  cast(NULL as DOUBLE) as dbus,
  y as dollar_dbus
from
  (-- Filter for active forecast run IDs and if there are multiple active forecast run IDs for a forecast_as_of / date, then average over them to avoid duplication
  select 
    forecast_as_of, ts_key, date, avg(y) as y
  from
    usage_forecasting_prod.forecast_results
  where 
    lower(is_active) = "true"
  group by
    forecast_as_of, ts_key, date)

-- COMMAND ----------

-- DBTITLE 1,Non-PVC forecast - padding in actuals
-- Padding in actuals till the forecast starts in each qtr
-- create table usage_forecasting_tab_silver.forecast_nonpvc_pad_actuals using delta as
insert overwrite usage_forecasting_tab_silver.forecast_nonpvc_pad_actuals

select
  'future forecast' as time_series_type,
  'future forecast - padded actuals' as time_series_type_detailed,
  'future forecast' as time_series_type_detailed_display,
  fcst_dates.forecast_date,
  act.platform,
  act.region,
  act.segment,
  act.sku,
  act.date,
  act.dbus,
  act.dollar_dbus
from
  (
    select
      *,
      CASE
        WHEN month(date_trunc("month", min_date)) = 1 THEN (year(date_trunc("year", min_date)) * 100) + 04
        WHEN month(date_trunc("month", min_date)) = 2 THEN ((year(date_trunc("year", min_date)) + 1) * 100) + 01
        WHEN month(date_trunc("month", min_date)) = 3 THEN ((year(date_trunc("year", min_date)) + 1) * 100) + 01
        WHEN month(date_trunc("month", min_date)) = 4 THEN ((year(date_trunc("year", min_date)) + 1) * 100) + 01
        WHEN month(date_trunc("month", min_date)) = 5 THEN ((year(date_trunc("year", min_date)) + 1) * 100) + 02
        WHEN month(date_trunc("month", min_date)) = 6 THEN ((year(date_trunc("year", min_date)) + 1) * 100) + 02
        WHEN month(date_trunc("month", min_date)) = 7 THEN ((year(date_trunc("year", min_date)) + 1) * 100) + 02
        WHEN month(date_trunc("month", min_date)) = 8 THEN ((year(date_trunc("year", min_date)) + 1) * 100) + 03
        WHEN month(date_trunc("month", min_date)) = 9 THEN ((year(date_trunc("year", min_date)) + 1) * 100) + 03
        WHEN month(date_trunc("month", min_date)) = 10 THEN ((year(date_trunc("year", min_date)) + 1) * 100) + 03
        WHEN month(date_trunc("month", min_date)) = 11 THEN ((year(date_trunc("year", min_date)) + 1) * 100) + 04
        WHEN month(date_trunc("month", min_date)) = 12 THEN ((year(date_trunc("year", min_date)) + 1) * 100) + 04
      END as curr_qtr_num
    from
      (
        select
          forecast_date,
          min(date) as min_date
        from
          usage_forecasting_tab_silver.forecast_nonpvc
        group by
          forecast_date
      )
  ) fcst_dates
  
  Cross Join
  
  (
    select
      *,
      CASE
        WHEN month(date_trunc("month", date)) = 1 THEN (year(date_trunc("year", date)) * 100) + 04
        WHEN month(date_trunc("month", date)) = 2 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
        WHEN month(date_trunc("month", date)) = 3 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
        WHEN month(date_trunc("month", date)) = 4 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
        WHEN month(date_trunc("month", date)) = 5 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
        WHEN month(date_trunc("month", date)) = 6 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
        WHEN month(date_trunc("month", date)) = 7 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
        WHEN month(date_trunc("month", date)) = 8 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
        WHEN month(date_trunc("month", date)) = 9 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
        WHEN month(date_trunc("month", date)) = 10 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
        WHEN month(date_trunc("month", date)) = 11 THEN ((year(date_trunc("year", date)) + 1) * 100) + 04
        WHEN month(date_trunc("month", date)) = 12 THEN ((year(date_trunc("year", date)) + 1) * 100) + 04
      END as date_qtr_num
    from
      (
        select
          *
        from
          usage_forecasting_tab_silver.actuals_non_pvc_pub

--         INCORRECT: Do not append PubSec actuals with Non-PVC. Treat PubSec along with PVC actuals.
--         union all

--         select
--           *
--         from
--           usage_forecasting_tab_silver.actuals_pubsec
      )
  ) act
where
  act.date_qtr_num = fcst_dates.curr_qtr_num
  and act.date < fcst_dates.min_date

-- COMMAND ----------

-- DBTITLE 1,PVC forecast
-- create table usage_forecasting_tab_silver.forecast_pvc_repeated using delta as
insert overwrite usage_forecasting_tab_silver.forecast_pvc_repeated

-- Append and dedup PVC forecast + actuals by Non-PVC forecast dates
select
  time_series_type,
  time_series_type_detailed,
  time_series_type_detailed_display,
  forecast_date,
  platform,
  region,
  segment,
  sku,
  date,
  dbus,
  dollar_dbus
from
(
  select 
    *,
    RANK() OVER (PARTITION BY forecast_date, platform, region, segment, sku, date ORDER BY time_series_type_detailed desc) AS rank
  from
  (
    select
      *
    from
    (
      -- PVC forecasts repeated for Non-PVC forecast_as_of dates
      select
        forecast_date,
        min_date,
        max_date,
        time_series_type,
        time_series_type_detailed,
        time_series_type_detailed_display,
        platform,
        region,
        segment,
        sku,
        date,
        dbus,
        dollar_dbus
      from
      (
        select
          forecast_date,
          pvc_forecast_date,
          min_date,
          max_date,
          time_series_type,
          time_series_type_detailed,
          time_series_type_detailed_display,
          platform,
          region,
          segment,
          sku,
          date,
          dbus,
          dollar_dbus,
          RANK() OVER (PARTITION BY forecast_date, platform, region, segment, sku ORDER BY pvc_forecast_date desc) AS nonpvc_fcst_rank_by_pvc_fcst
        from
          (
            select
              forecast_date,
              min(date) as min_date,
              max(date) as max_date,
              CASE
                WHEN month(date_trunc("month", forecast_date)) = 1 THEN (year(date_trunc("year", forecast_date)) * 100) + 04
                WHEN month(date_trunc("month", forecast_date)) = 2 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 01
                WHEN month(date_trunc("month", forecast_date)) = 3 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 01
                WHEN month(date_trunc("month", forecast_date)) = 4 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 01
                WHEN month(date_trunc("month", forecast_date)) = 5 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 02
                WHEN month(date_trunc("month", forecast_date)) = 6 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 02
                WHEN month(date_trunc("month", forecast_date)) = 7 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 02
                WHEN month(date_trunc("month", forecast_date)) = 8 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 03
                WHEN month(date_trunc("month", forecast_date)) = 9 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 03
                WHEN month(date_trunc("month", forecast_date)) = 10 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 03
                WHEN month(date_trunc("month", forecast_date)) = 11 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 04
                WHEN month(date_trunc("month", forecast_date)) = 12 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 04
              END as nonpvc_date_qtr_num
            from
              usage_forecasting_tab_silver.forecast_nonpvc
            group by
              forecast_date
          )

          cross join 

          (
            -- PVC forecasts
            select
              'future forecast' as time_series_type,
              'future forecast - pvc' as time_series_type_detailed,
              'future forecast - pvc' as time_series_type_detailed_display,
              forecast_as_of as pvc_forecast_date,
              'No Platform' as platform,
              case
                when lower(ts_key) LIKE "%amer%" then 'AMER'
                when lower(ts_key) LIKE "%apj%" then 'APAC'
                when lower(ts_key) LIKE "%emea%" then 'EMEA'
                when lower(ts_key) LIKE "%latam%" then 'LATAM'
                else 'No Region'
              end as region,
              case
                when trim(lower(ts_key)) LIKE "%enterprise%" then 'Enterprise'
                when trim(lower(ts_key)) LIKE "%mm/comm%" then 'MM/Comm'
                when trim(lower(ts_key)) LIKE "%pubsec%" then 'PubSec'
                when trim(lower(ts_key)) LIKE "%digital native%" then 'Digital Native'
                when trim(lower(ts_key)) LIKE "%all%" then 'All'
                else 'No Segment'
              end as segment,
              'No SKU' as sku,
              date,
              cast(NULL as DOUBLE) as dbus,
              avg(y) as dollar_dbus,
              CASE
                WHEN month(date_trunc("month", forecast_as_of)) = 1 THEN (year(date_trunc("year", forecast_as_of)) * 100) + 04
                WHEN month(date_trunc("month", forecast_as_of)) = 2 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 01
                WHEN month(date_trunc("month", forecast_as_of)) = 3 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 01
                WHEN month(date_trunc("month", forecast_as_of)) = 4 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 01
                WHEN month(date_trunc("month", forecast_as_of)) = 5 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 02
                WHEN month(date_trunc("month", forecast_as_of)) = 6 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 02
                WHEN month(date_trunc("month", forecast_as_of)) = 7 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 02
                WHEN month(date_trunc("month", forecast_as_of)) = 8 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 03
                WHEN month(date_trunc("month", forecast_as_of)) = 9 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 03
                WHEN month(date_trunc("month", forecast_as_of)) = 10 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 03
                WHEN month(date_trunc("month", forecast_as_of)) = 11 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 04
                WHEN month(date_trunc("month", forecast_as_of)) = 12 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 04
              END as pvc_forecast_qtr_num,
              CASE
                WHEN month(date_trunc("month", date)) = 1 THEN (year(date_trunc("year", date)) * 100) + 04
                WHEN month(date_trunc("month", date)) = 2 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
                WHEN month(date_trunc("month", date)) = 3 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
                WHEN month(date_trunc("month", date)) = 4 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
                WHEN month(date_trunc("month", date)) = 5 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
                WHEN month(date_trunc("month", date)) = 6 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
                WHEN month(date_trunc("month", date)) = 7 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
                WHEN month(date_trunc("month", date)) = 8 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
                WHEN month(date_trunc("month", date)) = 9 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
                WHEN month(date_trunc("month", date)) = 10 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
                WHEN month(date_trunc("month", date)) = 11 THEN ((year(date_trunc("year", date)) + 1) * 100) + 04
                WHEN month(date_trunc("month", date)) = 12 THEN ((year(date_trunc("year", date)) + 1) * 100) + 04
              END as pvc_date_qtr_num
            from
              usage_forecasting_prod.forecast_results_pvc
            where
              lower(is_active) = 'true'
            group by
              forecast_as_of, 
              case
                when lower(ts_key) LIKE "%amer%" then 'AMER'
                when lower(ts_key) LIKE "%apj%" then 'APAC'
                when lower(ts_key) LIKE "%emea%" then 'EMEA'
                when lower(ts_key) LIKE "%latam%" then 'LATAM'
                else 'No Region'
              end,
              case
                when trim(lower(ts_key)) LIKE "%enterprise%" then 'Enterprise'
                when trim(lower(ts_key)) LIKE "%mm/comm%" then 'MM/Comm'
                when trim(lower(ts_key)) LIKE "%pubsec%" then 'PubSec'
                when trim(lower(ts_key)) LIKE "%digital native%" then 'Digital Native'
                when trim(lower(ts_key)) LIKE "%all%" then 'All'
                else 'No Segment'
              end,
              date
          )
        where
          pvc_forecast_date <= forecast_date
          and pvc_forecast_qtr_num = nonpvc_date_qtr_num
          and pvc_date_qtr_num = nonpvc_date_qtr_num
        -- order by 
        --   forecast_date desc, platform, region, segment, sku, date desc, pvc_forecast_date desc, time_series_type_detailed
      )
      where 
        nonpvc_fcst_rank_by_pvc_fcst = 1
      -- order by 
      --   forecast_date desc, platform, region, segment, sku, date desc, pvc_forecast_date desc, time_series_type_detailed
    )

    union all

    (
      -- PVC actuals repeated for Non-PVC forecast_as_of dates
      select
        forecast_date,
        min_date,
        max_date,
        time_series_type,
        time_series_type_detailed,
        time_series_type_detailed_display,
        platform,
        region,
        segment,
        sku,
        date,
        dbus,
        dollar_dbus
      from
        (
          select
            forecast_date,
            min(date) as min_date,
            max(date) as max_date,
            CASE
              WHEN month(date_trunc("month", forecast_date)) = 1 THEN (year(date_trunc("year", forecast_date)) * 100) + 04
              WHEN month(date_trunc("month", forecast_date)) = 2 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 01
              WHEN month(date_trunc("month", forecast_date)) = 3 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 01
              WHEN month(date_trunc("month", forecast_date)) = 4 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 01
              WHEN month(date_trunc("month", forecast_date)) = 5 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 02
              WHEN month(date_trunc("month", forecast_date)) = 6 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 02
              WHEN month(date_trunc("month", forecast_date)) = 7 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 02
              WHEN month(date_trunc("month", forecast_date)) = 8 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 03
              WHEN month(date_trunc("month", forecast_date)) = 9 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 03
              WHEN month(date_trunc("month", forecast_date)) = 10 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 03
              WHEN month(date_trunc("month", forecast_date)) = 11 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 04
              WHEN month(date_trunc("month", forecast_date)) = 12 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 04
            END as nonpvc_date_qtr_num
          from
            usage_forecasting_tab_silver.forecast_nonpvc
          group by
            forecast_date
        )

        cross join 

        (
          -- PVC + PubSec Actuals
          select 
            *
          from
            (
              -- PVC Actuals
              select
                'future forecast' as time_series_type,
                'future forecast - padded actuals pvc' as time_series_type_detailed,
                'future forecast - pvc' as time_series_type_detailed_display,
                forecast_date as pvc_actuals_date,
                'No Platform' as platform,
                case
                  when upper(region) in ('APJ') then 'APAC'
                  else region
                end as region,
                case
                  when upper(region) in ('AMER') then segment
                  else 'All'
                end as segment,
                'No SKU' as sku,
                date,
                sum(dbus) as dbus,
                sum(dollar_dbus) as dollar_dbus,
                CASE
                  WHEN month(date_trunc("month", date)) = 1 THEN (year(date_trunc("year", date)) * 100) + 04
                  WHEN month(date_trunc("month", date)) = 2 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
                  WHEN month(date_trunc("month", date)) = 3 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
                  WHEN month(date_trunc("month", date)) = 4 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
                  WHEN month(date_trunc("month", date)) = 5 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
                  WHEN month(date_trunc("month", date)) = 6 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
                  WHEN month(date_trunc("month", date)) = 7 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
                  WHEN month(date_trunc("month", date)) = 8 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
                  WHEN month(date_trunc("month", date)) = 9 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
                  WHEN month(date_trunc("month", date)) = 10 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
                  WHEN month(date_trunc("month", date)) = 11 THEN ((year(date_trunc("year", date)) + 1) * 100) + 04
                  WHEN month(date_trunc("month", date)) = 12 THEN ((year(date_trunc("year", date)) + 1) * 100) + 04
                END as pvc_date_qtr_num
              from
                usage_forecasting_tab_silver.actuals_pvc
              group by
                forecast_date,
                region,
                segment,
                date
            )

            union all

            (
              -- PubSec Actuals
              select
                'future forecast' as time_series_type,
                'future forecast - padded actuals pubsec' as time_series_type_detailed,
                'future forecast - pvc' as time_series_type_detailed_display,
                forecast_date as pvc_actuals_date,
                'No Platform' as platform,
                case
                  when upper(region) in ('APJ') then 'APAC'
                  else region
                end as region,
                case
                  when upper(region) in ('AMER') then segment
                  else 'All'
                end as segment,
                'No SKU' as sku,
                date,
                sum(dbus) as dbus,
                sum(dollar_dbus) as dollar_dbus,
                CASE
                  WHEN month(date_trunc("month", date)) = 1 THEN (year(date_trunc("year", date)) * 100) + 04
                  WHEN month(date_trunc("month", date)) = 2 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
                  WHEN month(date_trunc("month", date)) = 3 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
                  WHEN month(date_trunc("month", date)) = 4 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
                  WHEN month(date_trunc("month", date)) = 5 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
                  WHEN month(date_trunc("month", date)) = 6 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
                  WHEN month(date_trunc("month", date)) = 7 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
                  WHEN month(date_trunc("month", date)) = 8 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
                  WHEN month(date_trunc("month", date)) = 9 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
                  WHEN month(date_trunc("month", date)) = 10 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
                  WHEN month(date_trunc("month", date)) = 11 THEN ((year(date_trunc("year", date)) + 1) * 100) + 04
                  WHEN month(date_trunc("month", date)) = 12 THEN ((year(date_trunc("year", date)) + 1) * 100) + 04
                END as pvc_date_qtr_num
              from
                usage_forecasting_tab_silver.actuals_pubsec
              group by
                forecast_date,
                region,
                segment,
                date
            )
        )
      where
        pvc_date_qtr_num = nonpvc_date_qtr_num
    )
  )
  order by 
    forecast_date desc, platform, region, segment, sku, date, time_series_type_detailed
)
where 
  rank = 1
-- order by 
--     forecast_date desc, platform, region, segment, sku, date, time_series_type_detailed

-- COMMAND ----------

-- DBTITLE 1,Finance forecasts - platform
-- create table usage_forecasting_tab_silver.fin_forecast_platform using delta as
insert overwrite usage_forecasting_tab_silver.fin_forecast_platform

select
  concat('finance forecast - ', forecastDimension) as time_series_type,
  concat('finance forecast - ', forecastDimension) as time_series_type_detailed,
  concat('finance forecast - ', forecastDimension) as time_series_type_detailed_display,
  to_date(asOfDate) as forecast_date,
  platform,
  cast(NULL as STRING) as region,
  cast(NULL as STRING) as segment,
  cast(NULL as STRING) as sku,
  date(
    concat(
      string(year),
      '-',
      lpad(string(month), 2, '0'),
      '-01'
    )
  ) as date,
  cast(NULL as DOUBLE) as dbus,
  dbuDollars as dollar_dbus
from
  finance_snap.dbu_dollar_finance_forecasts
where
  lower(forecastDimension) = 'platform'

-- COMMAND ----------

-- DBTITLE 1,Finance forecasts - BU
-- create table usage_forecasting_tab_silver.fin_forecast_bu using delta as
insert overwrite usage_forecasting_tab_silver.fin_forecast_bu

select
  concat('finance forecast - ', forecastDimension) as time_series_type,
  concat('finance forecast - ', forecastDimension) as time_series_type_detailed,
  concat('finance forecast - ', forecastDimension) as time_series_type_detailed_display,
  to_date(asOfDate) as forecast_date,
  cast(NULL as STRING) as platform,
  case
    when lower(split(platform, " ", 2) [0]) = 'apj' then 'APAC'
    else split(platform, " ", 2) [0]
  end as region,
  case
    when lower(split(platform, " ", 2) [1]) = 'mm/commercial' then 'MM/Comm'
    else split(platform, " ", 2) [1]
  end as segment,
  cast(NULL as STRING) as sku,
  date(
    concat(
      string(year),
      '-',
      lpad(string(month), 2, '0'),
      '-01'
    )
  ) as date,
  cast(NULL as DOUBLE) as dbus,
  dbuDollars as dollar_dbus
from
  finance_snap.dbu_dollar_finance_forecasts
where
  lower(forecastDimension) = 'business unit'

-- COMMAND ----------

-- DBTITLE 1,Gold - Tall table
-- create table usage_forecasting_tab_gold.tall_table using delta as
insert overwrite usage_forecasting_tab_gold.tall_table

select
  *
from
  (
    select
      *,
      date_format(date, "yyyy-MM") as mnt,
      CASE
        WHEN month(date_trunc("month", forecast_date)) = 1 THEN (year(date_trunc("year", forecast_date)) * 100) + 04
        WHEN month(date_trunc("month", forecast_date)) = 2 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 01
        WHEN month(date_trunc("month", forecast_date)) = 3 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 01
        WHEN month(date_trunc("month", forecast_date)) = 4 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 01
        WHEN month(date_trunc("month", forecast_date)) = 5 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 02
        WHEN month(date_trunc("month", forecast_date)) = 6 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 02
        WHEN month(date_trunc("month", forecast_date)) = 7 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 02
        WHEN month(date_trunc("month", forecast_date)) = 8 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 03
        WHEN month(date_trunc("month", forecast_date)) = 9 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 03
        WHEN month(date_trunc("month", forecast_date)) = 10 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 03
        WHEN month(date_trunc("month", forecast_date)) = 11 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 04
        WHEN month(date_trunc("month", forecast_date)) = 12 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 04
      END as fcst_qtr_num,
      CASE
        WHEN month(date_trunc("month", date)) = 1 THEN (year(date_trunc("year", date)) * 100) + 04
        WHEN month(date_trunc("month", date)) = 2 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
        WHEN month(date_trunc("month", date)) = 3 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
        WHEN month(date_trunc("month", date)) = 4 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
        WHEN month(date_trunc("month", date)) = 5 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
        WHEN month(date_trunc("month", date)) = 6 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
        WHEN month(date_trunc("month", date)) = 7 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
        WHEN month(date_trunc("month", date)) = 8 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
        WHEN month(date_trunc("month", date)) = 9 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
        WHEN month(date_trunc("month", date)) = 10 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
        WHEN month(date_trunc("month", date)) = 11 THEN ((year(date_trunc("year", date)) + 1) * 100) + 04
        WHEN month(date_trunc("month", date)) = 12 THEN ((year(date_trunc("year", date)) + 1) * 100) + 04
      END as qtr_num
    from
      (
        select
          *
        from
          usage_forecasting_tab_silver.actuals_combined 
        
        union all
        
        -- Appending in PVC forecasts
        select
          *
        from
          usage_forecasting_tab_silver.forecast_pvc_repeated
        
        union all
        
        select
          *
        from
          usage_forecasting_tab_silver.forecast_nonpvc
        
        union all
        
        -- Padding in actuals till the forecast starts in each qtr
        select
          *
        from
          usage_forecasting_tab_silver.forecast_nonpvc_pad_actuals
          
        union all
        
        select
          *
        from
          usage_forecasting_tab_silver.fin_forecast_platform
          
        union all
        
        select
          *
        from
          usage_forecasting_tab_silver.fin_forecast_bu
      )
  )
where
  lower(time_series_type) = 'actuals'
  OR fcst_qtr_num = qtr_num

-- COMMAND ----------

-- DBTITLE 1,QTD Acc - Finance forecast platform, Current qtr's first forecast date
-- create table usage_forecasting_tab_silver.fin_forecast_platform_curr_qtr_first_fcst_date using delta as
insert overwrite usage_forecasting_tab_silver.fin_forecast_platform_curr_qtr_first_fcst_date

select
  curr_qtr_first_forecast_date
from
  (
    select
      qtr_num,
      min(date) as curr_qtr_first_forecast_date
    from
      (
        select
          date,
          date(date_trunc("month", date)) as qtr_date,
          month(date_trunc("month", date)) as mnth,
          CASE
            WHEN month(date_trunc("month", date)) = 1 THEN (year(date_trunc("year", date)) * 100) + 04
            WHEN month(date_trunc("month", date)) = 2 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
            WHEN month(date_trunc("month", date)) = 3 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
            WHEN month(date_trunc("month", date)) = 4 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
            WHEN month(date_trunc("month", date)) = 5 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
            WHEN month(date_trunc("month", date)) = 6 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
            WHEN month(date_trunc("month", date)) = 7 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
            WHEN month(date_trunc("month", date)) = 8 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
            WHEN month(date_trunc("month", date)) = 9 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
            WHEN month(date_trunc("month", date)) = 10 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
            WHEN month(date_trunc("month", date)) = 11 THEN ((year(date_trunc("year", date)) + 1) * 100) + 04
            WHEN month(date_trunc("month", date)) = 12 THEN ((year(date_trunc("year", date)) + 1) * 100) + 04
          END as qtr_num
        from
          (
            select
              distinct date(asOfDate) as date
            from
              finance_snap.dbu_dollar_finance_forecasts
            where
              lower(forecastDimension) = 'platform'
          )
      )
    group by
      qtr_num
  )
where
  qtr_num in (
    select
      max(qtr_num) as most_recent_qtr_in_fin_forecast
    from
      (
        select
          qtr_num,
          min(date) as curr_qtr_first_forecast_date
        from
          (
            select
              date,
              date(date_trunc("month", date)) as qtr_date,
              month(date_trunc("month", date)) as mnth,
              CASE
                WHEN month(date_trunc("month", date)) = 1 THEN (year(date_trunc("year", date)) * 100) + 04
                WHEN month(date_trunc("month", date)) = 2 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
                WHEN month(date_trunc("month", date)) = 3 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
                WHEN month(date_trunc("month", date)) = 4 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
                WHEN month(date_trunc("month", date)) = 5 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
                WHEN month(date_trunc("month", date)) = 6 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
                WHEN month(date_trunc("month", date)) = 7 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
                WHEN month(date_trunc("month", date)) = 8 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
                WHEN month(date_trunc("month", date)) = 9 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
                WHEN month(date_trunc("month", date)) = 10 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
                WHEN month(date_trunc("month", date)) = 11 THEN ((year(date_trunc("year", date)) + 1) * 100) + 04
                WHEN month(date_trunc("month", date)) = 12 THEN ((year(date_trunc("year", date)) + 1) * 100) + 04
              END as qtr_num
            from
              (
                select
                  distinct date(asOfDate) as date
                from
                  finance_snap.dbu_dollar_finance_forecasts
                where
                  lower(forecastDimension) = 'platform'
              )
          )
        group by
          qtr_num
      )
  )

-- COMMAND ----------

-- DBTITLE 1,QTD Acc - DS forecast, Each qtr's first forecast date
-- create table usage_forecasting_tab_silver.ds_forecast_each_qtr_first_fcst_date using delta as 
insert overwrite usage_forecasting_tab_silver.ds_forecast_each_qtr_first_fcst_date

select
  qtr_num,
  min(date) as curr_qtr_first_forecast_date
from
  (
    select
      date,
      date(date_trunc("month", date)) as qtr_date,
      month(date_trunc("month", date)) as mnth,
      CASE
        WHEN month(date_trunc("month", date)) = 1 THEN (year(date_trunc("year", date)) * 100) + 04
        WHEN month(date_trunc("month", date)) = 2 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
        WHEN month(date_trunc("month", date)) = 3 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
        WHEN month(date_trunc("month", date)) = 4 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
        WHEN month(date_trunc("month", date)) = 5 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
        WHEN month(date_trunc("month", date)) = 6 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
        WHEN month(date_trunc("month", date)) = 7 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
        WHEN month(date_trunc("month", date)) = 8 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
        WHEN month(date_trunc("month", date)) = 9 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
        WHEN month(date_trunc("month", date)) = 10 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
        WHEN month(date_trunc("month", date)) = 11 THEN ((year(date_trunc("year", date)) + 1) * 100) + 04
        WHEN month(date_trunc("month", date)) = 12 THEN ((year(date_trunc("year", date)) + 1) * 100) + 04
      END as qtr_num
    from
      (
        select
          forecast_date as date
        from
          usage_forecasting_tab_silver.forecast_nonpvc
      )
  )
group by
  qtr_num

-- COMMAND ----------

-- DBTITLE 1,QTD Acc - DS forecast, Current qtr's first forecast date
-- create table usage_forecasting_tab_silver.ds_forecast_curr_qtr_first_fcst_date using delta as 
insert overwrite usage_forecasting_tab_silver.ds_forecast_curr_qtr_first_fcst_date

select
  curr_qtr_first_forecast_date
from
  usage_forecasting_tab_silver.ds_forecast_each_qtr_first_fcst_date
where
  qtr_num in 
  (
    select
      max(qtr_num) as most_recent_qtr_in_fin_forecast
    from
      usage_forecasting_tab_silver.ds_forecast_each_qtr_first_fcst_date
  )

-- COMMAND ----------

-- DBTITLE 1,QTD Acc - Finance forecast BU, Current qtr's first forecast date
-- create table usage_forecasting_tab_silver.fin_forecast_bu_curr_qtr_first_fcst_date using delta as
insert overwrite usage_forecasting_tab_silver.fin_forecast_bu_curr_qtr_first_fcst_date

select
  curr_qtr_first_forecast_date
from
  (
    select
      qtr_num,
      min(date) as curr_qtr_first_forecast_date
    from
      (
        select
          date,
          date(date_trunc("month", date)) as qtr_date,
          month(date_trunc("month", date)) as mnth,
          CASE
            WHEN month(date_trunc("month", date)) = 1 THEN (year(date_trunc("year", date)) * 100) + 04
            WHEN month(date_trunc("month", date)) = 2 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
            WHEN month(date_trunc("month", date)) = 3 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
            WHEN month(date_trunc("month", date)) = 4 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
            WHEN month(date_trunc("month", date)) = 5 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
            WHEN month(date_trunc("month", date)) = 6 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
            WHEN month(date_trunc("month", date)) = 7 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
            WHEN month(date_trunc("month", date)) = 8 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
            WHEN month(date_trunc("month", date)) = 9 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
            WHEN month(date_trunc("month", date)) = 10 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
            WHEN month(date_trunc("month", date)) = 11 THEN ((year(date_trunc("year", date)) + 1) * 100) + 04
            WHEN month(date_trunc("month", date)) = 12 THEN ((year(date_trunc("year", date)) + 1) * 100) + 04
          END as qtr_num
        from
          (
            select
              distinct date(asOfDate) as date
            from
              finance_snap.dbu_dollar_finance_forecasts
            where
              lower(forecastDimension) = 'business unit'
          )
      )
    group by
      qtr_num
  )
where
  qtr_num in (
    select
      max(qtr_num) as most_recent_qtr_in_fin_forecast
    from
      (
        select
          qtr_num,
          min(date) as curr_qtr_first_forecast_date
        from
          (
            select
              date,
              date(date_trunc("month", date)) as qtr_date,
              month(date_trunc("month", date)) as mnth,
              CASE
                WHEN month(date_trunc("month", date)) = 1 THEN (year(date_trunc("year", date)) * 100) + 04
                WHEN month(date_trunc("month", date)) = 2 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
                WHEN month(date_trunc("month", date)) = 3 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
                WHEN month(date_trunc("month", date)) = 4 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
                WHEN month(date_trunc("month", date)) = 5 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
                WHEN month(date_trunc("month", date)) = 6 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
                WHEN month(date_trunc("month", date)) = 7 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
                WHEN month(date_trunc("month", date)) = 8 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
                WHEN month(date_trunc("month", date)) = 9 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
                WHEN month(date_trunc("month", date)) = 10 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
                WHEN month(date_trunc("month", date)) = 11 THEN ((year(date_trunc("year", date)) + 1) * 100) + 04
                WHEN month(date_trunc("month", date)) = 12 THEN ((year(date_trunc("year", date)) + 1) * 100) + 04
              END as qtr_num
            from
              (
                select
                  distinct date(asOfDate) as date
                from
                  finance_snap.dbu_dollar_finance_forecasts
                where
                  lower(forecastDimension) = 'business unit'
              )
          )
        group by
          qtr_num
      )
  )

-- COMMAND ----------

-- DBTITLE 1,Gold - QTD Accuracy
-- create table usage_forecasting_tab_gold.qtd_accuracy using delta as
insert overwrite usage_forecasting_tab_gold.qtd_accuracy

select
  *
from
  (
    (
      select
        f1.time_series_type,
        f1.fin_forecast_date,
        a1.platform,
        a1.region,
        a1.segment,
        a1.sku,
        a1.date,
        f1.fin_dbus,
        f1.fin_dollar_dbus,
        a1.actual_dbus,
        a1.actual_dollar_dbus,
        d1.ds_forecast_date,
        d1.ds_dbus,
        d1.ds_dollar_dbus
      from
        (
          select
            platform,
            cast(NULL as STRING) as region,
            cast(NULL as STRING) as segment,
            cast(NULL as STRING) as sku,
            date(date_trunc("MM", date)) as date,
            sum(dbus) as actual_dbus,
            sum(dollar_dbus) as actual_dollar_dbus
          from
            usage_forecasting_tab_silver.actuals_combined
          group by
            platform,
            date(date_trunc("MM", date))
        ) a1
        
        left outer join 
        
        (
          select
            fin.*
          from
            (
              select 
                'platform' as time_series_type,
                forecast_date as fin_forecast_date,
                platform,
                region,
                segment,
                sku,
                date,
                dbus as fin_dbus,
                dollar_dbus as fin_dollar_dbus
              from usage_forecasting_tab_silver.fin_forecast_platform
            ) fin
            
            inner join 
            
            usage_forecasting_tab_silver.fin_forecast_platform_curr_qtr_first_fcst_date max 
            
            on fin.fin_forecast_date = max.curr_qtr_first_forecast_date
            
        ) f1
        on f1.platform = a1.platform
        and f1.date = a1.date
        
        left outer join
        
        (
          select
            platform,
            forecast_date as ds_forecast_date,
            date(date_trunc("MM", date)) as date,
            sum(dbus) as ds_dbus,
            sum(dollar_dbus) as ds_dollar_dbus
          from
            (
              select
                time_series_type,
                forecast_date,
                platform,
                region,
                segment,
                sku,
                date,
                dbus,
                dollar_dbus
              from
                usage_forecasting_tab_silver.forecast_nonpvc
            )
          where
            forecast_date in 
            (
                select *
                from usage_forecasting_tab_silver.ds_forecast_curr_qtr_first_fcst_date
            )
          group by
            platform,
            forecast_date,
            date(date_trunc("MM", date))
        ) d1 
        
        on d1.platform = a1.platform
        and d1.date = a1.date
    )
    
    union all
    
      (
        select
          f2.time_series_type,
          f2.forecast_date as fin_forecast_date,
          a2.platform,
          a2.region,
          a2.segment,
          a2.sku,
          a2.date,
          f2.fin_dbus,
          f2.fin_dollar_dbus,
          a2.actual_dbus,
          a2.actual_dollar_dbus,
          d2.ds_forecast_date,
          d2.ds_dbus,
          d2.ds_dollar_dbus
        from
          (
            select
              cast(NULL as STRING) as platform,
              case
                when upper(region) in ('APJ') then 'APAC'
                else region
              end as region,
              case
                when upper(region) in ('AMER') then segment
                else Null
              end as segment,
              cast(NULL as STRING) as sku,
              date(date_trunc("MM", date)) as date,
              sum(dbus) as actual_dbus,
              sum(dollar_dbus) as actual_dollar_dbus
            from
              usage_forecasting_tab_silver.actuals_combined act
            group by
              case
                when upper(region) in ('APJ') then 'APAC'
                else region
              end,
              case
                when upper(region) in ('AMER') then segment
                else Null
              end,
              date(date_trunc("MM", date))
          ) a2
          
          left outer join 
          
          (
            select
              fin.time_series_type,
              fin.forecast_date,
              fin.platform,
              case
                when lower(fin.region) = 'apj' then 'APAC'
                else fin.region
              end as region,
              case
                when lower(fin.segment) = 'mm/commercial' then 'MM/Comm'
                else fin.segment
              end as segment,
              fin.sku,
              fin.date,
              fin.fin_dbus,
              fin.fin_dollar_dbus
            from
              (
                select 
                  'business unit' as time_series_type,
                  forecast_date,
                  platform,
                  region,
                  segment,
                  sku,
                  date,
                  dbus as fin_dbus,
                  dollar_dbus as fin_dollar_dbus
                from usage_forecasting_tab_silver.fin_forecast_bu
              ) fin
              
              inner join 
              
              usage_forecasting_tab_silver.fin_forecast_bu_curr_qtr_first_fcst_date max 
              on fin.forecast_date = max.curr_qtr_first_forecast_date
              
          ) f2 
          
          on f2.region = a2.region
          and f2.segment IS NOT DISTINCT FROM a2.segment
          and f2.date = a2.date
          
          left outer join 
          
          (
            select 
              region,
              segment,
              ds_forecast_date,
              date,
              sum(ds_dbus) as ds_dbus,
              sum(ds_dollar_dbus) as ds_dollar_dbus
            from 
              (
                select
                  region,
                  case
                    when upper(region) in ('AMER') then segment
                    else Null
                  end as segment,
                  forecast_date as ds_forecast_date,
                  date(date_trunc("MM", date)) as date,
                  sum(dbus) as ds_dbus,
                  sum(dollar_dbus) as ds_dollar_dbus
                from
                  usage_forecasting_tab_silver.forecast_nonpvc
                where
                  forecast_date in 
                  (
                    select *
                    from usage_forecasting_tab_silver.ds_forecast_curr_qtr_first_fcst_date
                  )
                group by
                  region,
                  case
                    when upper(region) in ('AMER') then segment
                    else Null
                  end,
                  forecast_date,
                  date(date_trunc("MM", date))

                union all

                -- Appending the PVC forecasts to the Non-PVC forecasts
                select 
                  case
                    when lower(ts_key) LIKE "%amer%" then 'AMER'
                    when lower(ts_key) LIKE "%apj%" then 'APAC'
                    when lower(ts_key) LIKE "%emea%" then 'EMEA'
                    when lower(ts_key) LIKE "%latam%" then 'LATAM'
                    else 'No Region'
                  end as region,
                  case
                    when trim(lower(ts_key)) LIKE "%enterprise%" then 'Enterprise'
                    when trim(lower(ts_key)) LIKE "%mm/comm%" then 'MM/Comm'
                    when trim(lower(ts_key)) LIKE "%pubsec%" then 'PubSec'
                    when trim(lower(ts_key)) LIKE "%digital native%" then 'Digital Native'
                    when trim(lower(ts_key)) LIKE "%all%" and lower(ts_key) NOT LIKE "%amer%" then Null
                    when trim(lower(ts_key)) LIKE "%all%" and lower(ts_key) LIKE "%amer%" then 'All'
                    else 'No Segment'
                  end as segment,
                  ds_forecast_date,
                  date,
                  CAST(NULL as DOUBLE) as ds_dbus,
                  y as ds_dollar_dbus
                from
                  (
                    select
                      total.ts_key,
                      total.date,
                      total.y_lower,
                      total.y,
                      total.y_upper,
                      total.train_run_uuid
                    from
                      (
                        select
                          ts_key,
                          date,
                          forecast_as_of,
                          avg(y_lower) as y_lower,
                          avg(y) as y,
                          avg(y_upper) as y_upper,
                          first(train_run_uuid) as train_run_uuid
                        from
                          usage_forecasting_prod.forecast_results_pvc
                        where
                          lower(is_active) = 'true'
                        group by
                          ts_key,
                          date,
                          forecast_as_of
                      ) total

                      inner join 

                      (
                        select
                          ts_key,
                          min(forecast_as_of) as forecast_date
                        from
                          (
                            select
                              ts_key,
                              forecast_as_of,
                              CASE
                                WHEN month(date_trunc("month", forecast_as_of)) = 1 THEN (year(date_trunc("year", forecast_as_of)) * 100) + 04
                                WHEN month(date_trunc("month", forecast_as_of)) = 2 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 01
                                WHEN month(date_trunc("month", forecast_as_of)) = 3 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 01
                                WHEN month(date_trunc("month", forecast_as_of)) = 4 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 01
                                WHEN month(date_trunc("month", forecast_as_of)) = 5 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 02
                                WHEN month(date_trunc("month", forecast_as_of)) = 6 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 02
                                WHEN month(date_trunc("month", forecast_as_of)) = 7 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 02
                                WHEN month(date_trunc("month", forecast_as_of)) = 8 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 03
                                WHEN month(date_trunc("month", forecast_as_of)) = 9 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 03
                                WHEN month(date_trunc("month", forecast_as_of)) = 10 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 03
                                WHEN month(date_trunc("month", forecast_as_of)) = 11 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 04
                                WHEN month(date_trunc("month", forecast_as_of)) = 12 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 04
                              END as pvc_date_qtr_num,
                              CASE
                                  WHEN month(date_trunc("month", current_date())) = 1 THEN (year(date_trunc("year", current_date())) * 100) + 04
                                  WHEN month(date_trunc("month", current_date())) = 2 THEN ((year(date_trunc("year", current_date())) + 1) * 100) + 01
                                  WHEN month(date_trunc("month", current_date())) = 3 THEN ((year(date_trunc("year", current_date())) + 1) * 100) + 01
                                  WHEN month(date_trunc("month", current_date())) = 4 THEN ((year(date_trunc("year", current_date())) + 1) * 100) + 01
                                  WHEN month(date_trunc("month", current_date())) = 5 THEN ((year(date_trunc("year", current_date())) + 1) * 100) + 02
                                  WHEN month(date_trunc("month", current_date())) = 6 THEN ((year(date_trunc("year", current_date())) + 1) * 100) + 02
                                  WHEN month(date_trunc("month", current_date())) = 7 THEN ((year(date_trunc("year", current_date())) + 1) * 100) + 02
                                  WHEN month(date_trunc("month", current_date())) = 8 THEN ((year(date_trunc("year", current_date())) + 1) * 100) + 03
                                  WHEN month(date_trunc("month", current_date())) = 9 THEN ((year(date_trunc("year", current_date())) + 1) * 100) + 03
                                  WHEN month(date_trunc("month", current_date())) = 10 THEN ((year(date_trunc("year", current_date())) + 1) * 100) + 03
                                  WHEN month(date_trunc("month", current_date())) = 11 THEN ((year(date_trunc("year", current_date())) + 1) * 100) + 04
                                  WHEN month(date_trunc("month", current_date())) = 12 THEN ((year(date_trunc("year", current_date())) + 1) * 100) + 04
                                END as today_date_qtr_num
                            from
                              usage_forecasting_prod.forecast_results_pvc
                            group by
                              ts_key, forecast_as_of
                            having
                              pvc_date_qtr_num = today_date_qtr_num
                          )
                        group by
                          ts_key
                      ) latest 

                      on total.ts_key = latest.ts_key
                      and total.forecast_as_of = latest.forecast_date
                  )
                  
                  cross join
                  
                  (
                    select
                      min(forecast_date) as ds_forecast_date
                    from
                      (select
                        forecast_date,
                        CASE
                          WHEN month(date_trunc("month", forecast_date)) = 1 THEN (year(date_trunc("year", forecast_date)) * 100) + 04
                          WHEN month(date_trunc("month", forecast_date)) = 2 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 01
                          WHEN month(date_trunc("month", forecast_date)) = 3 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 01
                          WHEN month(date_trunc("month", forecast_date)) = 4 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 01
                          WHEN month(date_trunc("month", forecast_date)) = 5 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 02
                          WHEN month(date_trunc("month", forecast_date)) = 6 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 02
                          WHEN month(date_trunc("month", forecast_date)) = 7 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 02
                          WHEN month(date_trunc("month", forecast_date)) = 8 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 03
                          WHEN month(date_trunc("month", forecast_date)) = 9 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 03
                          WHEN month(date_trunc("month", forecast_date)) = 10 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 03
                          WHEN month(date_trunc("month", forecast_date)) = 11 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 04
                          WHEN month(date_trunc("month", forecast_date)) = 12 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 04
                        END as nonpvc_date_qtr_num,
                        CASE
                            WHEN month(date_trunc("month", current_date())) = 1 THEN (year(date_trunc("year", current_date())) * 100) + 04
                            WHEN month(date_trunc("month", current_date())) = 2 THEN ((year(date_trunc("year", current_date())) + 1) * 100) + 01
                            WHEN month(date_trunc("month", current_date())) = 3 THEN ((year(date_trunc("year", current_date())) + 1) * 100) + 01
                            WHEN month(date_trunc("month", current_date())) = 4 THEN ((year(date_trunc("year", current_date())) + 1) * 100) + 01
                            WHEN month(date_trunc("month", current_date())) = 5 THEN ((year(date_trunc("year", current_date())) + 1) * 100) + 02
                            WHEN month(date_trunc("month", current_date())) = 6 THEN ((year(date_trunc("year", current_date())) + 1) * 100) + 02
                            WHEN month(date_trunc("month", current_date())) = 7 THEN ((year(date_trunc("year", current_date())) + 1) * 100) + 02
                            WHEN month(date_trunc("month", current_date())) = 8 THEN ((year(date_trunc("year", current_date())) + 1) * 100) + 03
                            WHEN month(date_trunc("month", current_date())) = 9 THEN ((year(date_trunc("year", current_date())) + 1) * 100) + 03
                            WHEN month(date_trunc("month", current_date())) = 10 THEN ((year(date_trunc("year", current_date())) + 1) * 100) + 03
                            WHEN month(date_trunc("month", current_date())) = 11 THEN ((year(date_trunc("year", current_date())) + 1) * 100) + 04
                            WHEN month(date_trunc("month", current_date())) = 12 THEN ((year(date_trunc("year", current_date())) + 1) * 100) + 04
                          END as today_date_qtr_num
                      from 
                        usage_forecasting_tab_silver.forecast_nonpvc
                      group by
                        forecast_date
                      having
                        nonpvc_date_qtr_num = today_date_qtr_num)
                  )
              )
              group by
                region,
                segment,
                ds_forecast_date,
                date
          ) d2 
          
          on d2.region = a2.region
          and d2.segment IS NOT DISTINCT FROM a2.segment
          and d2.date = a2.date
      )
  ) dat
where
  actual_dollar_dbus is not null 
  and (ds_dollar_dbus is not null or 
       fin_dollar_dbus is not null)
  -- Exclude in-progress month as we cannot apportion the monthly finance forecast into weekly
  and date_trunc("MM", date) <> date_trunc("MM", current_date())
  -- Only on the 10th of a month should we include the actuals vs. forecast comparison for the full previous month since there is a lag in actuals of pvc data
  and datediff(to_date(from_utc_timestamp(current_timestamp(), 'PST')) , add_months(date, 1)) >= 9

-- COMMAND ----------

-- DBTITLE 1,Gold - Total qtr by forecast_date
-- create table usage_forecasting_tab_gold.total_qtr_by_forecast_date using delta as
insert overwrite usage_forecasting_tab_gold.total_qtr_by_forecast_date

select 
  *
from

-- Actuals for Non-PVC
(
  select
    'No Platform' as platform
    ,region
    ,segment
    ,'No SKU' as sku
    ,forecast_date
    ,'actuals' as forecast_type
    ,date
    ,avg(dbus) as dbus
    ,avg(dollar_dbus) as dollar_dbus
  from 
    (
      select 
        region,
        segment,
        forecast_date,
        date,
        data_pull_timestamp,
        fcst_qtr_num,
        sum(dbus) as dbus,
        sum(dollar_dbus) as dollar_dbus,
        CASE
          WHEN month(date_trunc("month", date)) = 1 THEN (year(date_trunc("year", date)) * 100) + 04
          WHEN month(date_trunc("month", date)) = 2 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
          WHEN month(date_trunc("month", date)) = 3 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
          WHEN month(date_trunc("month", date)) = 4 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
          WHEN month(date_trunc("month", date)) = 5 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
          WHEN month(date_trunc("month", date)) = 6 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
          WHEN month(date_trunc("month", date)) = 7 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
          WHEN month(date_trunc("month", date)) = 8 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
          WHEN month(date_trunc("month", date)) = 9 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
          WHEN month(date_trunc("month", date)) = 10 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
          WHEN month(date_trunc("month", date)) = 11 THEN ((year(date_trunc("year", date)) + 1) * 100) + 04
          WHEN month(date_trunc("month", date)) = 12 THEN ((year(date_trunc("year", date)) + 1) * 100) + 04
        END as actuals_qtr_num
      from
        usage_forecasting_tab_silver.frozen_actuals_non_pvc_pub
      group by
        region,
        segment,
        forecast_date,
        date,
        data_pull_timestamp,
        fcst_qtr_num
      )
  where
    -- forecast_date is the first date of forecast and actuals should be a day lesser than the first of forecast to avoid overlap / double counting
    date < forecast_date
    -- Only take the actuals for the 'current quarter', based on the forecast date
    and actuals_qtr_num = fcst_qtr_num
  group by
    region,
    segment,
    forecast_date,
    date
) 
  
union all

-- Actuals for PVC
(
  -- Actuals for PVC
  -- Only retain PVC actuals for dates which are before PVC forecasts (to avoid double counting with PVC forecasts)
  select 
    platform
    ,region
    ,segment
    ,sku
    ,forecast_date
    ,forecast_type
    ,date
    ,dbus
    ,dollar_dbus
  from
  (
    select
      act.platform
      ,act.region
      ,act.segment
      ,act.sku
      ,fcst.forecast_date
      ,act.forecast_type
      ,pvc_fcst_limit.pvc_forecast_date
      ,act.date
      ,dbus
      ,dollar_dbus
    --   ,fcst.min_date as fcst_min_date
    from
      (
        select
          'No Platform' as platform,
          region,
          segment,
          date,
          'No SKU' as sku,
          cast(NULL as DOUBLE) as dbus,
          sum(dollar_dbus) as dollar_dbus,
          'actuals' as forecast_type,
          CASE
            WHEN month(date_trunc("month", date)) = 1 THEN (year(date_trunc("year", date)) * 100) + 04
            WHEN month(date_trunc("month", date)) = 2 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
            WHEN month(date_trunc("month", date)) = 3 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
            WHEN month(date_trunc("month", date)) = 4 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
            WHEN month(date_trunc("month", date)) = 5 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
            WHEN month(date_trunc("month", date)) = 6 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
            WHEN month(date_trunc("month", date)) = 7 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
            WHEN month(date_trunc("month", date)) = 8 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
            WHEN month(date_trunc("month", date)) = 9 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
            WHEN month(date_trunc("month", date)) = 10 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
            WHEN month(date_trunc("month", date)) = 11 THEN ((year(date_trunc("year", date)) + 1) * 100) + 04
            WHEN month(date_trunc("month", date)) = 12 THEN ((year(date_trunc("year", date)) + 1) * 100) + 04
          END as qtr_num
        from
          usage_forecasting_tab_silver.actuals_combined
        where
          lower(time_series_type_detailed) = "actuals - pvc"
        group by
          region,
          segment,
          date
      ) act 
      --
      inner join 

      -- Forecast - distinct forecast dates
      (
        select
          forecast_date,
          --min(fcst_date) as min_date,
          forecast_date as min_date,
          CASE
            WHEN month(date_trunc("month", min(forecast_date))) = 1 THEN (year(date_trunc("year", min(forecast_date))) * 100) + 04
            WHEN month(date_trunc("month", min(forecast_date))) = 2 THEN ((year(date_trunc("year", min(forecast_date))) + 1) * 100) + 01
            WHEN month(date_trunc("month", min(forecast_date))) = 3 THEN ((year(date_trunc("year", min(forecast_date))) + 1) * 100) + 01
            WHEN month(date_trunc("month", min(forecast_date))) = 4 THEN ((year(date_trunc("year", min(forecast_date))) + 1) * 100) + 01
            WHEN month(date_trunc("month", min(forecast_date))) = 5 THEN ((year(date_trunc("year", min(forecast_date))) + 1) * 100) + 02
            WHEN month(date_trunc("month", min(forecast_date))) = 6 THEN ((year(date_trunc("year", min(forecast_date))) + 1) * 100) + 02
            WHEN month(date_trunc("month", min(forecast_date))) = 7 THEN ((year(date_trunc("year", min(forecast_date))) + 1) * 100) + 02
            WHEN month(date_trunc("month", min(forecast_date))) = 8 THEN ((year(date_trunc("year", min(forecast_date))) + 1) * 100) + 03
            WHEN month(date_trunc("month", min(forecast_date))) = 9 THEN ((year(date_trunc("year", min(forecast_date))) + 1) * 100) + 03
            WHEN month(date_trunc("month", min(forecast_date))) = 10 THEN ((year(date_trunc("year", min(forecast_date))) + 1) * 100) + 03
            WHEN month(date_trunc("month", min(forecast_date))) = 11 THEN ((year(date_trunc("year", min(forecast_date))) + 1) * 100) + 04
            WHEN month(date_trunc("month", min(forecast_date))) = 12 THEN ((year(date_trunc("year", min(forecast_date))) + 1) * 100) + 04
          END as min_qtr_num
        from
          (
            select
              concat(platform, region, segment, sku) as ts_key,
              'Ensemble' as model,
              '--DUMMY--' as run_id,
              date as fcst_date,
              dollar_dbus as y,
              forecast_date,
              time_series_type as for_forecast_type,
              time_series_type,
              platform,
              region,
              segment,
              sku
            from
              usage_forecasting_tab_silver.forecast_nonpvc
          ) fcst_inner
        group by
          forecast_date
        order by
          forecast_date
      ) fcst 

      on act.qtr_num = fcst.min_qtr_num
      and act.date < fcst.min_date

      left join

      (
        -- Combinations of Non-PVC forecast date and corresponding most-recent PVC forecast date
        select 
          distinct forecast_date, pvc_forecast_date
        from
        (
          select
            platform,
            region,
            segment,
            sku,
            forecast_date,
            pvc_forecast_date,
            'PVC forecast' as forecast_type,
            date,
            dbus,
            dollar_dbus
          from
          (
            select
              platform,
              region,
              segment,
              sku,
              forecast_date,
              pvc_forecast_date,
              'PVC forecast' as forecast_type,
              date,
              dbus,
              dollar_dbus,
              RANK() OVER (PARTITION BY forecast_date, platform, region, segment, sku ORDER BY pvc_forecast_date desc) AS nonpvc_fcst_rank_by_pvc_fcst
            from
              (
                select
                  forecast_date,
                  min(date) as min_date,
                  max(date) as max_date,
                  CASE
                    WHEN month(date_trunc("month", forecast_date)) = 1 THEN (year(date_trunc("year", forecast_date)) * 100) + 04
                    WHEN month(date_trunc("month", forecast_date)) = 2 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 01
                    WHEN month(date_trunc("month", forecast_date)) = 3 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 01
                    WHEN month(date_trunc("month", forecast_date)) = 4 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 01
                    WHEN month(date_trunc("month", forecast_date)) = 5 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 02
                    WHEN month(date_trunc("month", forecast_date)) = 6 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 02
                    WHEN month(date_trunc("month", forecast_date)) = 7 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 02
                    WHEN month(date_trunc("month", forecast_date)) = 8 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 03
                    WHEN month(date_trunc("month", forecast_date)) = 9 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 03
                    WHEN month(date_trunc("month", forecast_date)) = 10 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 03
                    WHEN month(date_trunc("month", forecast_date)) = 11 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 04
                    WHEN month(date_trunc("month", forecast_date)) = 12 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 04
                  END as nonpvc_date_qtr_num
                from
                  usage_forecasting_tab_silver.forecast_nonpvc
                group by
                  forecast_date
              )

              cross join 

              (
                -- PVC forecasts
                select
                  'future forecast' as time_series_type,
                  'future forecast - pvc' as time_series_type_detailed,
                  'future forecast - pvc' as time_series_type_detailed_display,
                  forecast_as_of as pvc_forecast_date,
                  'No Platform' as platform,
                  case
                    when lower(ts_key) LIKE "%amer%" then 'AMER'
                    when lower(ts_key) LIKE "%apj%" then 'APAC'
                    when lower(ts_key) LIKE "%emea%" then 'EMEA'
                    when lower(ts_key) LIKE "%latam%" then 'LATAM'
                    else 'No Region'
                  end as region,
                  case
                    when trim(lower(ts_key)) LIKE "%enterprise%" then 'Enterprise'
                    when trim(lower(ts_key)) LIKE "%mm/comm%" then 'MM/Comm'
                    when trim(lower(ts_key)) LIKE "%pubsec%" then 'PubSec'
                    when trim(lower(ts_key)) LIKE "%digital native%" then 'Digital Native'
                    when trim(lower(ts_key)) LIKE "%all%" then 'All'
                    else 'No Segment'
                  end as segment,
                  'No SKU' as sku,
                  date,
                  cast(NULL as DOUBLE) as dbus,
                  avg(y) as dollar_dbus,
                  CASE
                    WHEN month(date_trunc("month", forecast_as_of)) = 1 THEN (year(date_trunc("year", forecast_as_of)) * 100) + 04
                    WHEN month(date_trunc("month", forecast_as_of)) = 2 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 01
                    WHEN month(date_trunc("month", forecast_as_of)) = 3 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 01
                    WHEN month(date_trunc("month", forecast_as_of)) = 4 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 01
                    WHEN month(date_trunc("month", forecast_as_of)) = 5 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 02
                    WHEN month(date_trunc("month", forecast_as_of)) = 6 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 02
                    WHEN month(date_trunc("month", forecast_as_of)) = 7 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 02
                    WHEN month(date_trunc("month", forecast_as_of)) = 8 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 03
                    WHEN month(date_trunc("month", forecast_as_of)) = 9 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 03
                    WHEN month(date_trunc("month", forecast_as_of)) = 10 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 03
                    WHEN month(date_trunc("month", forecast_as_of)) = 11 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 04
                    WHEN month(date_trunc("month", forecast_as_of)) = 12 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 04
                  END as pvc_forecast_qtr_num,
                  CASE
                    WHEN month(date_trunc("month", date)) = 1 THEN (year(date_trunc("year", date)) * 100) + 04
                    WHEN month(date_trunc("month", date)) = 2 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
                    WHEN month(date_trunc("month", date)) = 3 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
                    WHEN month(date_trunc("month", date)) = 4 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
                    WHEN month(date_trunc("month", date)) = 5 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
                    WHEN month(date_trunc("month", date)) = 6 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
                    WHEN month(date_trunc("month", date)) = 7 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
                    WHEN month(date_trunc("month", date)) = 8 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
                    WHEN month(date_trunc("month", date)) = 9 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
                    WHEN month(date_trunc("month", date)) = 10 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
                    WHEN month(date_trunc("month", date)) = 11 THEN ((year(date_trunc("year", date)) + 1) * 100) + 04
                    WHEN month(date_trunc("month", date)) = 12 THEN ((year(date_trunc("year", date)) + 1) * 100) + 04
                  END as pvc_date_qtr_num
                from
                  usage_forecasting_prod.forecast_results_pvc
                where
                  lower(is_active) = 'true'
                group by
                  forecast_as_of, 
                  case
                    when lower(ts_key) LIKE "%amer%" then 'AMER'
                    when lower(ts_key) LIKE "%apj%" then 'APAC'
                    when lower(ts_key) LIKE "%emea%" then 'EMEA'
                    when lower(ts_key) LIKE "%latam%" then 'LATAM'
                    else 'No Region'
                  end,
                  case
                    when trim(lower(ts_key)) LIKE "%enterprise%" then 'Enterprise'
                    when trim(lower(ts_key)) LIKE "%mm/comm%" then 'MM/Comm'
                    when trim(lower(ts_key)) LIKE "%pubsec%" then 'PubSec'
                    when trim(lower(ts_key)) LIKE "%digital native%" then 'Digital Native'
                    when trim(lower(ts_key)) LIKE "%all%" then 'All'
                    else 'No Segment'
                  end,
                  date
              )
            where
              pvc_forecast_date <= forecast_date
              and pvc_forecast_qtr_num = nonpvc_date_qtr_num
              and pvc_date_qtr_num = nonpvc_date_qtr_num
              and date >= date_trunc("MM", forecast_date)
            order by
              forecast_date desc,
              region,
              segment,
              date desc,
              pvc_forecast_date desc
          )
          where
            nonpvc_fcst_rank_by_pvc_fcst = 1
        )
      ) pvc_fcst_limit

      on 
      fcst.forecast_date = pvc_fcst_limit.forecast_date
  )
  -- Only retain PVC actuals for dates which are before PVC forecasts
  where 
    date < pvc_forecast_date
)

union all
  
  
-- Non-PVC Forecasts for future dates
(
  select
    fcst_inner.platform
    ,fcst_inner.region
    ,fcst_inner.segment
    ,fcst_inner.sku
    ,fcst_inner.forecast_date
    ,fcst_inner.forecast_type
    ,fcst_inner.date
    ,cast(NULL as DOUBLE) as dbus
    ,fcst_inner.dollar_dbus
  from
    (
      select
        concat(platform, region, segment, sku) as ts_key,
        'Ensemble' as model,
        '--DUMMY--' as run_id,
        date,
        dollar_dbus,
        forecast_date,
        time_series_type as forecast_type,
        time_series_type,
        platform,
        region,
        segment,
        sku
      from
        usage_forecasting_tab_silver.forecast_nonpvc
    ) fcst_inner
  where
    date >= forecast_date
  --   order by forecast_date, date
)

union all

(
  -- PVC forecasts repeated for Non-PVC forecast_as_of dates
  select
    platform,
    region,
    segment,
    sku,
    forecast_date,
    'PVC forecast' as forecast_type,
    date,
    dbus,
    dollar_dbus
  from
  (
    select
      platform,
      region,
      segment,
      sku,
      forecast_date,
      pvc_forecast_date,
      'PVC forecast' as forecast_type,
      date,
      dbus,
      dollar_dbus,
      RANK() OVER (PARTITION BY forecast_date, platform, region, segment, sku ORDER BY pvc_forecast_date desc) AS nonpvc_fcst_rank_by_pvc_fcst
    from
      (
        select
          forecast_date,
          min(date) as min_date,
          max(date) as max_date,
          CASE
            WHEN month(date_trunc("month", forecast_date)) = 1 THEN (year(date_trunc("year", forecast_date)) * 100) + 04
            WHEN month(date_trunc("month", forecast_date)) = 2 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 01
            WHEN month(date_trunc("month", forecast_date)) = 3 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 01
            WHEN month(date_trunc("month", forecast_date)) = 4 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 01
            WHEN month(date_trunc("month", forecast_date)) = 5 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 02
            WHEN month(date_trunc("month", forecast_date)) = 6 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 02
            WHEN month(date_trunc("month", forecast_date)) = 7 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 02
            WHEN month(date_trunc("month", forecast_date)) = 8 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 03
            WHEN month(date_trunc("month", forecast_date)) = 9 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 03
            WHEN month(date_trunc("month", forecast_date)) = 10 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 03
            WHEN month(date_trunc("month", forecast_date)) = 11 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 04
            WHEN month(date_trunc("month", forecast_date)) = 12 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 04
          END as nonpvc_date_qtr_num
        from
          usage_forecasting_tab_silver.forecast_nonpvc
        group by
          forecast_date
      )

      cross join 

      (
        -- PVC forecasts
        select
          'future forecast' as time_series_type,
          'future forecast - pvc' as time_series_type_detailed,
          'future forecast - pvc' as time_series_type_detailed_display,
          forecast_as_of as pvc_forecast_date,
          'No Platform' as platform,
          case
            when lower(ts_key) LIKE "%amer%" then 'AMER'
            when lower(ts_key) LIKE "%apj%" then 'APAC'
            when lower(ts_key) LIKE "%emea%" then 'EMEA'
            when lower(ts_key) LIKE "%latam%" then 'LATAM'
            else 'No Region'
          end as region,
          case
            when trim(lower(ts_key)) LIKE "%enterprise%" then 'Enterprise'
            when trim(lower(ts_key)) LIKE "%mm/comm%" then 'MM/Comm'
            when trim(lower(ts_key)) LIKE "%pubsec%" then 'PubSec'
            when trim(lower(ts_key)) LIKE "%digital native%" then 'Digital Native'
            when trim(lower(ts_key)) LIKE "%all%" then 'All'
            else 'No Segment'
          end as segment,
          'No SKU' as sku,
          date,
          cast(NULL as DOUBLE) as dbus,
          avg(y) as dollar_dbus,
          CASE
            WHEN month(date_trunc("month", forecast_as_of)) = 1 THEN (year(date_trunc("year", forecast_as_of)) * 100) + 04
            WHEN month(date_trunc("month", forecast_as_of)) = 2 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 01
            WHEN month(date_trunc("month", forecast_as_of)) = 3 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 01
            WHEN month(date_trunc("month", forecast_as_of)) = 4 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 01
            WHEN month(date_trunc("month", forecast_as_of)) = 5 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 02
            WHEN month(date_trunc("month", forecast_as_of)) = 6 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 02
            WHEN month(date_trunc("month", forecast_as_of)) = 7 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 02
            WHEN month(date_trunc("month", forecast_as_of)) = 8 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 03
            WHEN month(date_trunc("month", forecast_as_of)) = 9 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 03
            WHEN month(date_trunc("month", forecast_as_of)) = 10 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 03
            WHEN month(date_trunc("month", forecast_as_of)) = 11 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 04
            WHEN month(date_trunc("month", forecast_as_of)) = 12 THEN ((year(date_trunc("year", forecast_as_of)) + 1) * 100) + 04
          END as pvc_forecast_qtr_num,
          CASE
            WHEN month(date_trunc("month", date)) = 1 THEN (year(date_trunc("year", date)) * 100) + 04
            WHEN month(date_trunc("month", date)) = 2 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
            WHEN month(date_trunc("month", date)) = 3 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
            WHEN month(date_trunc("month", date)) = 4 THEN ((year(date_trunc("year", date)) + 1) * 100) + 01
            WHEN month(date_trunc("month", date)) = 5 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
            WHEN month(date_trunc("month", date)) = 6 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
            WHEN month(date_trunc("month", date)) = 7 THEN ((year(date_trunc("year", date)) + 1) * 100) + 02
            WHEN month(date_trunc("month", date)) = 8 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
            WHEN month(date_trunc("month", date)) = 9 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
            WHEN month(date_trunc("month", date)) = 10 THEN ((year(date_trunc("year", date)) + 1) * 100) + 03
            WHEN month(date_trunc("month", date)) = 11 THEN ((year(date_trunc("year", date)) + 1) * 100) + 04
            WHEN month(date_trunc("month", date)) = 12 THEN ((year(date_trunc("year", date)) + 1) * 100) + 04
          END as pvc_date_qtr_num
        from
          usage_forecasting_prod.forecast_results_pvc
        where
          lower(is_active) = 'true'
        group by
          forecast_as_of, 
          case
            when lower(ts_key) LIKE "%amer%" then 'AMER'
            when lower(ts_key) LIKE "%apj%" then 'APAC'
            when lower(ts_key) LIKE "%emea%" then 'EMEA'
            when lower(ts_key) LIKE "%latam%" then 'LATAM'
            else 'No Region'
          end,
          case
            when trim(lower(ts_key)) LIKE "%enterprise%" then 'Enterprise'
            when trim(lower(ts_key)) LIKE "%mm/comm%" then 'MM/Comm'
            when trim(lower(ts_key)) LIKE "%pubsec%" then 'PubSec'
            when trim(lower(ts_key)) LIKE "%digital native%" then 'Digital Native'
            when trim(lower(ts_key)) LIKE "%all%" then 'All'
            else 'No Segment'
          end,
          date
      )
    where
      pvc_forecast_date <= forecast_date
      and pvc_forecast_qtr_num = nonpvc_date_qtr_num
      and pvc_date_qtr_num = nonpvc_date_qtr_num
      and date >= 
        (
          -- Pick PVC forecasts only for dates after the latest PVC actuals date
          select 
            max(date)
          from 
          (
            select
              *
            from
              usage_forecasting_tab_silver.actuals_pubsec

            union all

            select
              *
            from
              usage_forecasting_tab_silver.actuals_pvc
          )
        )
    order by
      forecast_date desc,
      region,
      segment,
      date desc,
      pvc_forecast_date desc
  )
  where
    nonpvc_fcst_rank_by_pvc_fcst = 1
)

order by forecast_date, date

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Tableau Parameter Automation Metadata queries

-- COMMAND ----------

-- DBTITLE 1,QTD Accuracy Metadata - Extract overlap months
-- Tech debt, can remove since this functionality is not used?
create or replace temp view raw_qtd_overlap_months as
select
  distinct date,
  date_format(date, 'MMMM yyyy') as months
from
  usage_forecasting_tab_gold.qtd_accuracy

-- COMMAND ----------

-- DBTITLE 1,QTD Accuracy Metadata - Concatenate overlap months into a cell
-- MAGIC %python
-- MAGIC # Tech debt, can remove since this functionality is not used?
-- MAGIC 
-- MAGIC from pyspark.sql.functions import collect_list, concat_ws
-- MAGIC 
-- MAGIC spark_df = spark.table("raw_qtd_overlap_months").sort("date")
-- MAGIC grouped_df = spark_df.agg(collect_list('months').alias("qtd_acc_combined_overlap_months"))
-- MAGIC 
-- MAGIC grouped_df.withColumn("qtd_acc_combined_overlap_months", concat_ws(", ", "qtd_acc_combined_overlap_months")).createOrReplaceTempView("qtd_acc_combined_overlap_months_tbl")
-- MAGIC display(spark.table("qtd_acc_combined_overlap_months_tbl"))

-- COMMAND ----------

-- DBTITLE 1,Tableau Refresh Date Metadata table
-- create table usage_forecasting_tab_gold.dashboard_metadata using delta as
insert overwrite usage_forecasting_tab_gold.dashboard_metadata

select
  *
from
  (
    select
      from_utc_timestamp(current_timestamp(), 'PST') as dashboard_refresh_date
  ),
  (
    select
      max(forecast_date) as latest_nonpvc_forecast_date
    from 
      usage_forecasting_tab_silver.forecast_nonpvc
  ),
  (
    select
      max(forecast_as_of) as latest_pvc_forecast_date
    from 
      usage_forecasting_prod.forecast_results_pvc
  ),
  (
    select
      min(forecast_date) as first_nonpvc_forecast_date_in_qtr
    from
      (select
        forecast_date,
        CASE
          WHEN month(date_trunc("month", forecast_date)) = 1 THEN (year(date_trunc("year", forecast_date)) * 100) + 04
          WHEN month(date_trunc("month", forecast_date)) = 2 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 01
          WHEN month(date_trunc("month", forecast_date)) = 3 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 01
          WHEN month(date_trunc("month", forecast_date)) = 4 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 01
          WHEN month(date_trunc("month", forecast_date)) = 5 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 02
          WHEN month(date_trunc("month", forecast_date)) = 6 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 02
          WHEN month(date_trunc("month", forecast_date)) = 7 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 02
          WHEN month(date_trunc("month", forecast_date)) = 8 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 03
          WHEN month(date_trunc("month", forecast_date)) = 9 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 03
          WHEN month(date_trunc("month", forecast_date)) = 10 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 03
          WHEN month(date_trunc("month", forecast_date)) = 11 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 04
          WHEN month(date_trunc("month", forecast_date)) = 12 THEN ((year(date_trunc("year", forecast_date)) + 1) * 100) + 04
        END as nonpvc_date_qtr_num,
        CASE
            WHEN month(date_trunc("month", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) = 1 THEN (year(date_trunc("year", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) * 100) + 04
            WHEN month(date_trunc("month", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) = 2 THEN ((year(date_trunc("year", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) + 1) * 100) + 01
            WHEN month(date_trunc("month", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) = 3 THEN ((year(date_trunc("year", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) + 1) * 100) + 01
            WHEN month(date_trunc("month", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) = 4 THEN ((year(date_trunc("year", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) + 1) * 100) + 01
            WHEN month(date_trunc("month", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) = 5 THEN ((year(date_trunc("year", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) + 1) * 100) + 02
            WHEN month(date_trunc("month", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) = 6 THEN ((year(date_trunc("year", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) + 1) * 100) + 02
            WHEN month(date_trunc("month", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) = 7 THEN ((year(date_trunc("year", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) + 1) * 100) + 02
            WHEN month(date_trunc("month", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) = 8 THEN ((year(date_trunc("year", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) + 1) * 100) + 03
            WHEN month(date_trunc("month", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) = 9 THEN ((year(date_trunc("year", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) + 1) * 100) + 03
            WHEN month(date_trunc("month", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) = 10 THEN ((year(date_trunc("year", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) + 1) * 100) + 03
            WHEN month(date_trunc("month", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) = 11 THEN ((year(date_trunc("year", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) + 1) * 100) + 04
            WHEN month(date_trunc("month", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) = 12 THEN ((year(date_trunc("year", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) + 1) * 100) + 04
          END as today_date_qtr_num
      from 
        usage_forecasting_tab_silver.forecast_nonpvc
      group by
        forecast_date
      having
        nonpvc_date_qtr_num = today_date_qtr_num)
  ),
  (
    select
      date(min(asOfDate)) as first_finance_forecast_date_in_qtr
    from
      (select
        asOfDate,
        CASE
          WHEN month(date_trunc("month", asOfDate)) = 1 THEN (year(date_trunc("year", asOfDate)) * 100) + 04
          WHEN month(date_trunc("month", asOfDate)) = 2 THEN ((year(date_trunc("year", asOfDate)) + 1) * 100) + 01
          WHEN month(date_trunc("month", asOfDate)) = 3 THEN ((year(date_trunc("year", asOfDate)) + 1) * 100) + 01
          WHEN month(date_trunc("month", asOfDate)) = 4 THEN ((year(date_trunc("year", asOfDate)) + 1) * 100) + 01
          WHEN month(date_trunc("month", asOfDate)) = 5 THEN ((year(date_trunc("year", asOfDate)) + 1) * 100) + 02
          WHEN month(date_trunc("month", asOfDate)) = 6 THEN ((year(date_trunc("year", asOfDate)) + 1) * 100) + 02
          WHEN month(date_trunc("month", asOfDate)) = 7 THEN ((year(date_trunc("year", asOfDate)) + 1) * 100) + 02
          WHEN month(date_trunc("month", asOfDate)) = 8 THEN ((year(date_trunc("year", asOfDate)) + 1) * 100) + 03
          WHEN month(date_trunc("month", asOfDate)) = 9 THEN ((year(date_trunc("year", asOfDate)) + 1) * 100) + 03
          WHEN month(date_trunc("month", asOfDate)) = 10 THEN ((year(date_trunc("year", asOfDate)) + 1) * 100) + 03
          WHEN month(date_trunc("month", asOfDate)) = 11 THEN ((year(date_trunc("year", asOfDate)) + 1) * 100) + 04
          WHEN month(date_trunc("month", asOfDate)) = 12 THEN ((year(date_trunc("year", asOfDate)) + 1) * 100) + 04
        END as finance_date_qtr_num,
        CASE
            WHEN month(date_trunc("month", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) = 1 THEN (year(date_trunc("year", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) * 100) + 04
            WHEN month(date_trunc("month", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) = 2 THEN ((year(date_trunc("year", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) + 1) * 100) + 01
            WHEN month(date_trunc("month", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) = 3 THEN ((year(date_trunc("year", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) + 1) * 100) + 01
            WHEN month(date_trunc("month", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) = 4 THEN ((year(date_trunc("year", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) + 1) * 100) + 01
            WHEN month(date_trunc("month", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) = 5 THEN ((year(date_trunc("year", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) + 1) * 100) + 02
            WHEN month(date_trunc("month", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) = 6 THEN ((year(date_trunc("year", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) + 1) * 100) + 02
            WHEN month(date_trunc("month", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) = 7 THEN ((year(date_trunc("year", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) + 1) * 100) + 02
            WHEN month(date_trunc("month", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) = 8 THEN ((year(date_trunc("year", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) + 1) * 100) + 03
            WHEN month(date_trunc("month", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) = 9 THEN ((year(date_trunc("year", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) + 1) * 100) + 03
            WHEN month(date_trunc("month", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) = 10 THEN ((year(date_trunc("year", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) + 1) * 100) + 03
            WHEN month(date_trunc("month", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) = 11 THEN ((year(date_trunc("year", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) + 1) * 100) + 04
            WHEN month(date_trunc("month", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) = 12 THEN ((year(date_trunc("year", to_date(from_utc_timestamp(current_timestamp(), 'PST')))) + 1) * 100) + 04
          END as today_date_qtr_num
      from 
        finance_snap.dbu_dollar_finance_forecasts
      group by
        asOfDate
      having
        finance_date_qtr_num = today_date_qtr_num)
  ),
  (
    -- Tech debt, can remove since this functionality is not used?
    select 
      *
    from 
      qtd_acc_combined_overlap_months_tbl
  ),
  (
    select
      date_format(to_date(from_utc_timestamp(current_timestamp(), 'PST')), "MMMM yyyy") as qtd_acc_current_month
  ),
  (
    select
      next_day(from_utc_timestamp(current_timestamp(), 'PST'), "monday") as next_ds_refresh_date
  ),
  (
    select
      date_add(add_months(max(date), 1), 9) as qtd_latest_refresh_date
    from
      usage_forecasting_tab_gold.qtd_accuracy
  )

-- COMMAND ----------

-- DBTITLE 1,Display latest refresh date metadata
select
  *
from 
  usage_forecasting_tab_gold.dashboard_metadata
