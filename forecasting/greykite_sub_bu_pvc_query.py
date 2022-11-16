sql_quey = "WITH AMER_ENT AS ( \
  SELECT \
    month, \
    year,CASE \
      WHEN SFDC_REGION_L2 IS NULL \
      OR SFDC_REGION_L2 IN ('OHV/Southeast', 'Northeast') THEN 'AMER_Enterprise_Other' \
      else replace( \
        concat('AMER_Enterprise_', SFDC_REGION_L2), \
        \" \", \
        \"_\" \
      ) \
    end as sub_bu, \
    SUM(dbuDollars) AS DOLLAR_DBU \
  FROM \
    ( \
      select \
        month, \
        year, \
        dbuDollars, \
        bu, \
        SFDC_REGION_L1,\
        SFDC_REGION_L2 \
      from \
        finance.pvc_usage \
      union all \
      select \
        month, \
        year, \
        dbuDollars, \
        bu, \
        SFDC_REGION_L1, \
        SFDC_REGION_L2 \
      from \
        finance.pubsec_usage \
    ) \
  WHERE \
    BU = 'AMER Enterprise' \
  group by \
    month, \
    year, \
    sub_bu \
), \
AMER_GSC AS ( \
  SELECT \
    month, \
    year, \
    replace(concat(\"AMER_GSC_\", SFDC_REGION_L1), \" \", \"_\") as sub_bu, \
    SUM(dbuDollars) AS DOLLAR_DBU \
  FROM \
    ( \
      select \
        month, \
        year, \
        dbuDollars, \
        bu, \
        SFDC_REGION_L1, \
        SFDC_REGION_L2 \
      from \
        finance.pvc_usage \
      union all \
      select \
        month, \
        year, \
        dbuDollars, \
        bu, \
        SFDC_REGION_L1, \
        SFDC_REGION_L2 \
      from \
        finance.pubsec_usage \
    ) \
  WHERE \
    SFDC_REGION_L1 = 'Startup' \
    AND BU = 'AMER GSC' \
  group by \
    month, \
    year, \
    sub_bu \
  UNION ALL \
  SELECT \
    month, \
    year, \
    replace(concat(\"AMER_GSC_\", SFDC_REGION_L2), \" \", \"_\") as sub_bu, \
    SUM(dbuDollars) AS DOLLAR_DBU \
  FROM \
    ( \
      select \
        month, \
        year, \
        dbuDollars, \
        bu, \
        SFDC_REGION_L1, \
        SFDC_REGION_L2 \
      from \
        finance.pvc_usage \
      union all \
      select \
        month, \
        year, \
        dbuDollars, \
        bu, \
        SFDC_REGION_L1, \
        SFDC_REGION_L2 \
      from \
        finance.pubsec_usage \
    ) \
  WHERE \
    SFDC_REGION_L1 != 'Startup' \
    AND BU = 'AMER GSC' \
  group by \
    month, \
    year, \
    sub_bu \
), \
AMER_Regulated_Verticals AS ( \
  SELECT \
    month, \
    year, \
    replace( \
      concat(\"AMER_Regulated_Verticals_\", SFDC_REGION_L1), \
      \" \", \
      \"_\" \
    ) as sub_bu, \
    SUM(dbuDollars) AS DOLLAR_DBU \
  FROM \
    ( \
      select \
        month, \
        year, \
        dbuDollars, \
        bu, \
        SFDC_REGION_L1, \
        SFDC_REGION_L2 \
      from \
        finance.pvc_usage \
      union all \
      select \
        month, \
        year, \
        dbuDollars, \
        bu, \
        SFDC_REGION_L1, \
        SFDC_REGION_L2 \
      from \
        finance.pubsec_usage \
    ) \
  WHERE \
    BU = 'AMER Regulated Verticals' \
  group by \
    month, \
    year, \
    sub_bu \
), \
EMEA ( \
  SELECT \
    month, \
    year, \
    CASE \
      WHEN SFDC_REGION_L1 = 'EMEA' THEN 'EMEA_Other' \
      else replace(concat(\"EMEA_\", SFDC_REGION_L1), \" \", \"_\") \
    end as sub_bu, \
    SUM(dbuDollars) AS DOLLAR_DBU \
  FROM \
    ( \
      select \
        month, \
        year, \
        dbuDollars, \
        bu, \
        SFDC_REGION_L1, \
        SFDC_REGION_L2 \
      from \
        finance.pvc_usage \
      union all \
      select \
        month, \
        year, \
        dbuDollars, \
        bu, \
        SFDC_REGION_L1, \
        SFDC_REGION_L2 \
      from \
        finance.pubsec_usage \
    ) \
  WHERE \
    BU = 'EMEA' \
  group by \
    month, \
    year, \
    sub_bu \
), \
APJ ( \
  SELECT \
    month, \
    year, \
    replace(concat(\"APJ_\", SFDC_REGION_L1), \" \", \"_\") as sub_bu, \
    SUM(dbuDollars) AS DOLLAR_DBU \
  FROM \
    ( \
      select \
        month, \
        year, \
        dbuDollars, \
        bu, \
        SFDC_REGION_L1, \
        SFDC_REGION_L2 \
      from \
        finance.pvc_usage \
      union all \
      select \
        month, \
        year, \
        dbuDollars, \
        bu, \
        SFDC_REGION_L1, \
        SFDC_REGION_L2 \
      from \
        finance.pubsec_usage \
    ) \
  WHERE \
    BU = 'APJ' \
    AND SFDC_REGION_L1 IN ('ANZ', 'Japan') \
  group by \
    month, \
    year, \
    sub_bu \
  UNION ALL \
  SELECT \
    month, \
    year, \
    CASE \
      WHEN SFDC_REGION_L2 not IN (\"Asean\", \"India\", \"GCR\", \"Korea\") \
      or SFDC_REGION_L2 is null then 'APJ_Other' \
      else replace(concat(\"APJ_\", SFDC_REGION_L2), \" \", \"_\") \
    end as sub_bu, \
    SUM(dbuDollars) AS DOLLAR_DBU \
  FROM \
    ( \
      select \
        month, \
        year, \
        dbuDollars, \
        bu, \
        SFDC_REGION_L1, \
        SFDC_REGION_L2 \
      from \
        finance.pvc_usage \
      union all \
      select \
        month, \
        year, \
        dbuDollars, \
        bu, \
        SFDC_REGION_L1, \
        SFDC_REGION_L2 \
      from \
        finance.pubsec_usage \
    ) \
  WHERE \
    BU = 'APJ' \
    AND SFDC_REGION_L1 NOT IN ('ANZ', 'Japan') \
  group by \
    month, \
    year, \
    sub_bu \
), \
DIGITAL_NATIVE AS ( \
  SELECT \
    month, \
    year, \
    replace(BU, \" \", \"_\") as sub_bu, \
    SUM(dbuDollars) AS DOLLAR_DBU \
  FROM \
    ( \
      select \
        month, \
        year, \
        dbuDollars, \
        bu, \
        SFDC_REGION_L1, \
        SFDC_REGION_L2 \
      from \
        finance.pvc_usage \
      union all \
      select \
        month, \
        year, \
        dbuDollars, \
        bu, \
        SFDC_REGION_L1, \
        SFDC_REGION_L2 \
      from \
        finance.pubsec_usage \
    ) \
  WHERE \
    BU = 'Digital Native' \
  group by \
    month, \
    year, \
    sub_bu \
), \
MSFT AS ( \
  SELECT \
    month, \
    year, \
    replace(BU, \" \", \"_\") as sub_bu, \
    SUM(dbuDollars) AS DOLLAR_DBU \
  FROM \
    ( \
      select \
        month, \
        year, \
        dbuDollars, \
        bu, \
        SFDC_REGION_L1, \
        SFDC_REGION_L2 \
      from \
        finance.pvc_usage \
      union all \
      select \
        month, \
        year, \
        dbuDollars, \
        bu, \
        SFDC_REGION_L1, \
        SFDC_REGION_L2 \
      from \
        finance.pubsec_usage \
    ) \
  WHERE \
    BU = 'Microsoft House Account' \
  group by \
    month, \
    year, \
    sub_bu \
), \
final ( \
  SELECT \
    * \
  FROM \
    AMER_ENT \
  UNION ALL \
  SELECT \
    * \
  FROM \
    AMER_GSC \
  UNION ALL \
  SELECT \
    * \
  FROM \
    AMER_Regulated_Verticals \
  UNION ALL \
  SELECT \
    * \
  FROM \
    EMEA \
  UNION ALL \
  SELECT \
    * \
  FROM \
    APJ \
  UNION ALL \
  SELECT \
    * \
  FROM \
    DIGITAL_NATIVE \
  UNION ALL \
  SELECT \
    * \
  FROM \
    MSFT \
) \
select \
  month, \
  year, \
  sub_bu, \
  sum(DOLLAR_DBU) as DOLLAR_DBU \
from \
  final \
group by \
  month, \
  year, \
  sub_bu"