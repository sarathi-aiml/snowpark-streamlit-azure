--I've set up a scheduled task to extract summarized data from Snowflake's usage metrics views and store it in my personal database. This strategy helps me prevent time-out issues when working with Streamlit and Azure app service.

CREATE TASK refresh_WarehouseCreditsOverTime_task
  WAREHOUSE ='SB_DW_XS'
  SCHEDULE='USING CRON 0 23 * * * UTC'
  AS
  CREATE OR REPLACE TABLE WarehouseCreditsOverTime AS 
  SELECT 
      TO_CHAR(start_time,'YYYY-MM') AS mnth, 
      SUM(credits_used) AS sum_cr
  FROM 
      snowflake.account_usage.warehouse_metering_history wmh 
  WHERE 
      wmh.start_time >= DATEADD(month, -12, CURRENT_DATE())
  GROUP BY 
      TO_CHAR(start_time,'YYYY-MM');


ALTER TASK refresh_WarehouseCreditsOverTime_task RESUME;

select * from WarehouseCreditsOverTime;

CREATE TASK refresh_CreditsbyWarehouse_task
  WAREHOUSE = 'SB_DW_XS'
  SCHEDULE = 'USING CRON 0 23 * * * UTC'
  AS
  CREATE OR REPLACE TABLE WarehouseCreditUsage AS 
  SELECT 
      warehouse_name, 
      SUM(credits_used) AS credits_used
  FROM 
      snowflake.account_usage.warehouse_metering_history wmh 
  GROUP BY 
      warehouse_name
  HAVING 
      SUM(credits_used) BETWEEN 1000 AND 5000
  LIMIT 10; -- limiting to 10 for test case

  ALTER TASK refresh_CreditsbyWarehouse_task RESUME;

  select * from WarehouseCreditUsage;

  CREATE TASK refresh_MonthlyCreditsbyType_task
  WAREHOUSE = 'SB_DW_XS'
  SCHEDULE = 'USING CRON 0 23 * * * UTC'
  AS
  CREATE OR REPLACE TABLE MonthlyCreditsbyType AS 
  SELECT 
      TO_CHAR(usage_date,'YYYYMM') as month,
      SUM(DECODE(service_type, 'WAREHOUSE_METERING', credits_billed)) as warehouse_credits,
      SUM(DECODE(service_type, 'PIPE', credits_billed)) as pipe_credits,
      SUM(DECODE(service_type, 'MATERIALIZED_VIEW', credits_billed)) as mview_credits,
      SUM(DECODE(service_type, 'AUTO_CLUSTERING', credits_billed)) as clustering_credits,
      SUM(DECODE(service_type, 'WAREHOUSE_METERING_READER', credits_billed)) as reader_credits,
      SUM(credits_billed) as total
  FROM 
      snowflake.account_usage.metering_daily_history wmh
  WHERE 
      wmh.usage_date >= DATEADD(month, -12, CURRENT_DATE())
  GROUP BY 
      TO_CHAR(usage_date,'YYYYMM');


  ALTER TASK refresh_MonthlyCreditsbyType_task RESUME;

  select * from MonthlyCreditsbyType;

  CREATE TASK refresh_HourlyCreditUsage_task
  WAREHOUSE = 'SB_DW_XS'
  SCHEDULE = 'USING CRON 0 23 * * * UTC'
  AS
  CREATE OR REPLACE TABLE HourlyCreditUsage AS 
  SELECT 
      TO_CHAR(start_time,'HH24') AS hour, 
      SUM(credits_used) AS cr
  FROM 
      snowflake.account_usage.warehouse_metering_history wmh 
  WHERE 
      wmh.start_time >= DATEADD(month, -1, CURRENT_DATE())
  GROUP BY 
      TO_CHAR(start_time,'HH24')
  ORDER BY 1;

  ALTER TASK refresh_HourlyCreditUsage_task RESUME;


  select * from HourlyCreditUsage;

  CREATE TASK refresh_MonthlyStorageUsage_task
  WAREHOUSE = 'SB_DW_XS'
  SCHEDULE = 'USING CRON 0 23 * * * UTC'
  AS
  CREATE OR REPLACE TABLE MonthlyStorageUsage AS 
  SELECT 
      TO_CHAR(usage_date,'YYYYMM') AS sort_month,
      TO_CHAR(usage_date,'Mon-YYYY') AS month,
      AVG(storage_bytes) AS storage,
      AVG(stage_bytes) AS stage,
      AVG(failsafe_bytes) AS failsafe
  FROM 
      snowflake.account_usage.storage_usage
  GROUP BY 
      month, sort_month
  ORDER BY 
      sort_month;

      ALTER TASK refresh_MonthlyStorageUsage_task RESUME;

select * from MonthlyStorageUsage;

show users;
