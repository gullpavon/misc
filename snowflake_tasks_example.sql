
/* Blueprint ETL Task

Note: Snowflake task can only call 1 SP, so you have to write one task for each procedure. LAWL. 

MONTH END /CURR DATA DUPE FIX FOR TABLEAU

*/

execute task TASK_BLUEPRINT_1_ETL_SPCLIENT



--STEP 1: RUN spClient: This creates t_client_info_curr which is general data about the client, i.e cust_keys, address, age, officer. (Customer Level deatails) 
CREATE OR REPLACE TASK TASK_BLUEPRINT_1_ETL_SPCLIENT
WAREHOUSE =  'LOAD_WH'
SCHEDULE = 'USING CRON 0 8 * * * America/Los_Angeles' -- USE http://www.cronmaker.com/;jsessionid=node01jfgbcdpvcuyq11hdbfo9xv9yz104070.node0?0 -- format minute/hour/day of momnth/month/ day of week 
AS
CALL spClient();



ALTER TASK "TASK_BLUEPRINT_1_ETL_SPCLIENT" resume;


--STEP 2: RUN ClientAcctHist: this creates acct lvl details i.e: deposit/loan/pwm balances, has customer key (this will be for month end only for the last year and year end only for anything past that)
CREATE OR REPLACE TASK TASK_BLUEPRINT_2_ETL_CLIENT_ACCT_HIST
WAREHOUSE =  'LOAD_WH'
AFTER TASK_BLUEPRINT_1_ETL_SPCLIENT
--SCHEDULE = -- RUN DAILY TO CAPTURE PWM CHANGES
AS
CALL ClientAcctHist()
;

--STEP 3: RUN this creates acct lvl details i.e: deposit/loan/pwm balances, has customer key run daily (as of today)
CREATE OR REPLACE TASK TASK_BLUEPRINT_3_ETL_CLIENT_ACCT_CURR
WAREHOUSE =  'LOAD_WH'
--SCHEDULE = -- RUN DAILY TO CAPTURE PWM CHANGES
AFTER TASK_BLUEPRINT_1_ETL_SPCLIENT
AS
CALL ClientAcctCurr(TO_VARCHAR(DATEADD(DAY,-1,CURRENT_DATE)))
;


--STEP 4: RUN AcctSecondariesCurr: this gets all secondaries who have ownership on the accounts, i.e. spouse
CREATE OR REPLACE TASK TASK_BLUEPRINT_4_ETL_ACCT_SECONDARIES_CURR
WAREHOUSE =  'LOAD_WH'
AFTER TASK_BLUEPRINT_1_ETL_SPCLIENT
--SCHEDULE = -- RUN DAILY 
AS
CALL AcctSecondariesCurr()
;




--STEP 6: technically indepdent on spclient but run daily anyways 

CREATE OR REPLACE TASK TASK_BLUEPRINT_5_ETL_BONUS_INSERT
WAREHOUSE =  'LOAD_WH'
--SCHEDULE = --RUN DAILY
AFTER TASK_BLUEPRINT_1_ETL_SPCLIENT
AS
CALL BLUEPRINT_BONUS_INSERT()

;



--STEP 7: determines whether or not a client has payroll, this is used for potential client population, who may be enrolled via string matching. THIS IS A TRUNC AND RELOAD  technically indepdent on spclient but run daily anyways 
CREATE OR REPLACE TASK TASK_BLUEPRINT_6_ETL_PAYROLL_STAGE
WAREHOUSE =  'LOAD_WH'
AFTER TASK_BLUEPRINT_1_ETL_SPCLIENT
--SCHEDULE = --RUN weekly on saturday  -- DAILY BUT INDEPENDENT OF EVERYTHING EXCEPT BLUEPRINT COMPANY NAME TABLE

AS
call BLUEPRINT_PAYROLL_STAGE(TO_VARCHAR(DATEADD(DAY,-1,CURRENT_DATE)))
;


--STEP 8: trunc and reload, this is if they are recieving direct deposit doesnt matter from where.  katie uses data for a report on (16th) that determines if bankers will be paid out bonuses based if the client has dd.  technically indepdent on spclient but run daily anyways 
CREATE OR REPLACE TASK TASK_BLUEPRINT_7_ETL_BP_DD_STAGE
WAREHOUSE =  'LOAD_WH'
-- schedule =  runs on the 15th every month 
AFTER TASK_BLUEPRINT_1_ETL_SPCLIENT
AS
CALL BLUEPRINT_BP_DD_STAGE(TO_VARCHAR(DATEADD(DAY,-1,CURRENT_DATE)))

;

--STEP 9:
CREATE OR REPLACE TASK TASK_BLUEPRINT_8_ETL_NEW_CLIENTS
WAREHOUSE =  'LOAD_WH'
AFTER TASK_BLUEPRINT_1_ETL_SPCLIENT -- DAILY BUT INDEPENDENT OF EVERYTHING EXCEPT BLUEPRINT COMPANY NAME TABLE
AS
CALL BLUEPRINT_NEW_CLIENTS(TO_VARCHAR(DATEADD(DAY,-1,CURRENT_DATE)))



--STEP10

CREATE OR REPLACE TASK TASK_BLUEPRINT_9_ETL_TXN_COUNT
WAREHOUSE =  'LOAD_WH'
AFTER TASK_BLUEPRINT_1_ETL_SPCLIENT -- DAILY BUT INDEPENDENT OF EVERYTHING EXCEPT BLUEPRINT COMPANY NAME TABLE
AS
CALL BLUEPRINT_TXN_COUNT_STAGE(TO_VARCHAR(DATEADD(DAY,-1,CURRENT_DATE)))


--ALTER TASK "TASK_BLUEPRINT_1_ETL_SPCLIENT" suspend;

ALTER TASK "TASK_BLUEPRINT_1_ETL_SPCLIENT" RESUME;
ALTER TASK "TASK_BLUEPRINT_2_ETL_CLIENT_ACCT_HIST" RESUME;
ALTER TASK "TASK_BLUEPRINT_3_ETL_CLIENT_ACCT_CURR" RESUME;
ALTER TASK "TASK_BLUEPRINT_4_ETL_ACCT_SECONDARIES_CURR" RESUME;
ALTER TASK "TASK_BLUEPRINT_5_ETL_BONUS_INSERT" RESUME;
ALTER TASK "TASK_BLUEPRINT_6_ETL_PAYROLL_STAGE" RESUME;
ALTER TASK "TASK_BLUEPRINT_7_ETL_BP_DD_STAGE" RESUME;
ALTER TASK "TASK_BLUEPRINT_8_ETL_NEW_CLIENTS" RESUME;
ALTER TASK "TASK_BLUEPRINT_9_ETL_TXN_COUNT" RESUME;



ALTER TASK "TASK_BLUEPRINT_3_ETL_CLIENTACCTCURR" SUSPEND;
DROP TASK TASK_BLUEPRINT_3_ETL_CLIENTACCTCURR;
DROP TASK TASK_BLUEPRINT_2_ETL_CLIENTACCTHIST;
DROP TASK MY_TEST_TASK_ROLE;
DROP TASK TASK_BLUEPRINT_6_ETL_BONUS_INSERT;

show tasks

select 
NAME
,STATE
,QUERY_START_TIME
,NEXT_SCHEDULED_TIME
,COMPLETED_TIME
,SCHEDULED_FROM
,ERROR_MESSAGE

select
* 
from table(information_schema.task_history())
where schema_name like 'REPORTING_GPAVON'
and name like '%TEAMLEADER_START%'