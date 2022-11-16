sql_quey = "WITH AMER_ENT AS (\
  SELECT\
    DATE,CASE\
      WHEN SFDC_REGION_L2 IS NULL\
      OR SFDC_REGION_L2 IN ('OHV/Southeast', 'Northeast') THEN 'AMER_Enterprise_Other'\
      else concat('AMER_Enterprise_', SFDC_REGION_L2)\
    end as sub_bu,\
    SUM(dbuDollars) AS DOLLAR_DBU\
  FROM\
    finance.dbu_dollars\
  WHERE\
    BU = 'AMER Enterprise'\
  group by\
    DATE,\
    sub_bu\
),\
AMER_GSC AS (\
  SELECT\
    DATE,\
    concat(\"AMER_GSC_\", SFDC_REGION_L1) as sub_bu,\
    SUM(dbuDollars) AS DOLLAR_DBU\
  FROM\
    finance.dbu_dollars\
  WHERE\
    SFDC_REGION_L1 = 'Startup'\
    AND BU = 'AMER GSC'\
  group by\
    DATE,\
    sub_bu\
  UNION ALL\
  SELECT\
    DATE,\
    concat(\"AMER_GSC_\", SFDC_REGION_L2) as sub_bu,\
    SUM(dbuDollars) AS DOLLAR_DBU\
  FROM\
    finance.dbu_dollars\
  WHERE\
    SFDC_REGION_L1 != 'Startup'\
    AND BU = 'AMER GSC'\
  group by\
    DATE,\
    sub_bu\
),\
AMER_Regulated_Verticals AS (\
  SELECT\
    DATE,\
    concat(\"AMER_Regulated_Verticals_\", SFDC_REGION_L1) as sub_bu,\
    SUM(dbuDollars) AS DOLLAR_DBU\
  FROM\
    finance.dbu_dollars\
  WHERE\
    BU = 'AMER Regulated Verticals'\
  group by\
    DATE,\
    sub_bu\
),\
EMEA (\
  SELECT\
    DATE,CASE\
      WHEN SFDC_REGION_L1 = 'EMEA' THEN 'EMEA_Other'\
      else concat(\"EMEA_\", SFDC_REGION_L1)\
    end as sub_bu,\
    SUM(dbuDollars) AS DOLLAR_DBU\
  FROM\
    finance.dbu_dollars\
  WHERE\
    BU = 'EMEA'\
  group by\
    DATE,\
    sub_bu\
),\
APJ (\
  SELECT\
    DATE,\
    concat(\"APJ_\", SFDC_REGION_L1) as sub_bu,\
    SUM(dbuDollars) AS DOLLAR_DBU\
  FROM\
    finance.dbu_dollars\
  WHERE\
    BU = 'APJ'\
    AND SFDC_REGION_L1 IN ('ANZ', 'Japan')\
  group by\
    DATE,\
    sub_bu\
  UNION ALL\
  SELECT\
    DATE,CASE\
      WHEN SFDC_REGION_L2 not IN (\"Asean\", \"India\", \"GCR\", \"Korea\")\
      or SFDC_REGION_L2 is null then 'APJ_Other'\
      else concat(\"APJ_\", SFDC_REGION_L2)\
    end as sub_bu,\
    SUM(dbuDollars) AS DOLLAR_DBU\
  FROM\
    finance.dbu_dollars\
  WHERE\
    BU = 'APJ'\
    AND SFDC_REGION_L1 NOT IN ('ANZ', 'Japan')\
  group by\
    DATE,\
    sub_bu\
),\
DIGITAL_NATIVE AS (\
  SELECT\
    DATE,\
    BU as sub_bu,\
    SUM(dbuDollars) AS DOLLAR_DBU\
  FROM\
    finance.dbu_dollars\
  WHERE\
    BU = 'Digital Native'\
  group by\
    DATE,\
    sub_bu\
),\
MSFT AS (\
  SELECT\
    DATE,\
    BU as sub_bu,\
    SUM(dbuDollars) AS DOLLAR_DBU\
  FROM\
    finance.dbu_dollars\
  WHERE\
    BU = 'Microsoft House Account'\
  group by\
    DATE,\
    sub_bu\
),\
final\
(\
SELECT * FROM AMER_ENT\
UNION ALL\
SELECT * FROM AMER_GSC\
UNION ALL\
SELECT * FROM AMER_Regulated_Verticals\
UNION ALL\
SELECT * FROM EMEA\
UNION ALL\
SELECT * FROM APJ\
UNION ALL\
SELECT * FROM DIGITAL_NATIVE\
UNION ALL\
SELECT * FROM MSFT\
)\
select date, sub_bu, sum(dollar_dbu) as dollar_dbu from final\
group by date, sub_bu"

