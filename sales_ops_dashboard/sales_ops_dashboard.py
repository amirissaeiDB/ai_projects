# Databricks notebook source
# MAGIC %md
# MAGIC # Sales Ops dashboard (used to track accounts' movements).
# MAGIC Basic visualization on how accounts moves through time. Sales leaders will be able to detect big changes and dig deeper in their accounts. For dashboard, see [here](https://us-west-2b.online.tableau.com/#/site/databrickstableaucom/views/Account-levelDSforecast-WIP/Q323DSForecast?:iid=1)

# COMMAND ----------

# MAGIC %sql create
# MAGIC or replace table usage_forecasting_prod.sales_ops_dashboard as (
# MAGIC   with mapping as (
# MAGIC     select
# MAGIC       distinct accountId,
# MAGIC       accountName,
# MAGIC       bu,
# MAGIC       sub_bu
# MAGIC     from
# MAGIC       finance.dbu_dollars
# MAGIC   ),
# MAGIC   forecast as (
# MAGIC     select
# MAGIC       *
# MAGIC     from
# MAGIC       usage_forecasting_acct.account_date_forecast_history
# MAGIC     where
# MAGIC       date <= '2022-10-31'
# MAGIC       and date >= '2022-08-01'
# MAGIC       and sfdcAccountId not in (
# MAGIC         select
# MAGIC           distinct accountID
# MAGIC         from
# MAGIC           finance.pvc_usage
# MAGIC         union
# MAGIC         select
# MAGIC           distinct accountID
# MAGIC         from
# MAGIC           finance.pubsec_usage
# MAGIC       )
# MAGIC   ),
# MAGIC   actual_to_date as (
# MAGIC     select
# MAGIC       date,
# MAGIC       accountId,
# MAGIC       sum(dbuDollars) as actual
# MAGIC     from
# MAGIC       finance.dbu_dollars
# MAGIC     group by
# MAGIC       date,
# MAGIC       accountId
# MAGIC   ),
# MAGIC   actual_filtered as (
# MAGIC     select
# MAGIC       a.accountId,
# MAGIC       accountName,
# MAGIC       bu,
# MAGIC       sub_bu,
# MAGIC       runDate,
# MAGIC       actual as Forecast
# MAGIC     from
# MAGIC       actual_to_date as a
# MAGIC       inner join mapping as b on a.accountId = b.accountId
# MAGIC       cross join (
# MAGIC         select
# MAGIC           distinct runDate
# MAGIC         from
# MAGIC           forecast
# MAGIC       ) as c
# MAGIC     where
# MAGIC       a.date < c.runDate
# MAGIC       and a.date >= '2022-08-01'
# MAGIC   ),
# MAGIC   max_date as (
# MAGIC     select
# MAGIC       max(runDate) as max_rundate
# MAGIC     from
# MAGIC       forecast
# MAGIC   )
# MAGIC   select
# MAGIC     *
# MAGIC   from
# MAGIC     (
# MAGIC       select
# MAGIC         accountId,
# MAGIC         accountName,
# MAGIC         bu,
# MAGIC         sub_bu,
# MAGIC         runDate,
# MAGIC         forecast as Forecast,
# MAGIC         date_add(cast(max_rundate as date), 4) as date_refreshed,
# MAGIC         date_add(cast(max_rundate as date), 11) as next_update_date
# MAGIC       from
# MAGIC         forecast as a
# MAGIC         inner join mapping as b on a.sfdcAccountID = b.accountId
# MAGIC         cross join max_date
# MAGIC     )
# MAGIC   union all
# MAGIC   select
# MAGIC     accountId,
# MAGIC     accountName,
# MAGIC     bu,
# MAGIC     sub_bu,
# MAGIC     runDate,
# MAGIC     forecast as Forecast,
# MAGIC     date_add(cast(max_rundate as date), 4) as date_refreshed,
# MAGIC     date_add(cast(max_rundate as date), 11) as next_update_date
# MAGIC   from
# MAGIC     actual_filtered
# MAGIC     cross join max_date
# MAGIC )
