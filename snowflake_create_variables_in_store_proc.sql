USE ROLE GG_SNOWFLAKE_REPORTING_GPAVON;
USE WAREHOUSE FRB_WH;
USE DATABASE EDCI_SANDBOX;
USE SCHEMA REPORTING_GPAVON;

---call SP_TL_TABLEAU_OUTPUT_SPLITS('jherbert', '2022-03-13')



CREATE or replace PROCEDURE SP_TL_TABLEAU_OUTPUT_SPLITS(LOGIN VARCHAR, DATE_PARAM VARCHAR)
RETURNS VARCHAR
LANGUAGE javascript
EXECUTE AS CALLER
AS
$$
snowflake.execute({"sqlText": `SET LOGIN = '${LOGIN}'`});
snowflake.execute({"sqlText": `SET PROD_DT_SELECT = '${DATE_PARAM}'`});
snowflake.execute({"sqlText": "SET DATE_KEY = (SELECT REPLACE($PROD_DT_SELECT,'-',''))"});
snowflake.execute({"sqlText": "SET PROD_DT = TO_DATE($PROD_DT_SELECT)"});
snowflake.execute({"sqlText": "SET BEG_OF_YEAR = date_trunc('YEAR', TO_DATE($PROD_DT_SELECT))"});
snowflake.execute({"sqlText": "SET PROD_DT_EOM = (SELECT TOP 1 PROD_DT FROM RDM_LOAN.SILVER_MART.V_FACT_LOAN_EOM WHERE DATE_KEY <= $DATE_KEY ORDER BY prod_dt DESC)"});


snowflake.execute({"sqlText": "SET PROD_DT_PREV_EOM = DATEADD(MONTH, -1 ,LAST_DAY($PROD_DT))"});
snowflake.execute({"sqlText": "SET PROD_DT_PREV_QUARTER_EOM = DATEADD(QUARTER, -1 ,LAST_DAY($PROD_DT))"});
snowflake.execute({"sqlText": "SET PROD_DT_PREV_YEAR_EOM = DATEADD(YEAR, -1 ,LAST_DAY($PROD_DT))"});

var rs = snowflake.execute( { sqlText: 

`


/*---------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------


							Team-to-Individual Split view:


----------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------*/



--CREATE OR REPLACE TABLE TL_TABLEAU_OUTPUT_SPLITS AS
INSERT INTO TL_TABLEAU_OUTPUT_SPLITS

SELECT  
		DISTINCT TABLEAU_OUTPUT.INITIALS	
		,TABLEAU_OUTPUT.SELECTED_OFFICER_LOGIN
	    ,TABLEAU_OUTPUT.SELECTED_OFFICER_NAME
	    ,RM_TEAMSPLIT_NUMBERS.*

	   
--INTO DataScienceAndAnalytics.teamlead.tableau_output_splits

FROM EDCI_SANDBOX.REPORTING_GPAVON.TL_TABLEAU_OUTPUT TABLEAU_OUTPUT
LEFT JOIN (

	   /*
	   The below ensures that any employee that is associated with a team gets their respective split % added into their individual's total values.
	   */
			SELECT
			  
				   BALANCES.PROD_DT
				  ,TEAM_REF.INITIALS																			AS INITIALS_TR
				  ,FRB_PLP.NAME																					AS "Individual Name"
				  ,TEAM_REF.SHAREDINITIALS																		AS "Shared Initials"
				  ,TEAM_REF.NAME																				AS Name
				  ,TEAM_REF.SPLIT																				AS Split
				  ,SUM(ifnull(BALANCES.NEW_CHECKING_CURR,0) * TEAM_REF.SPLIT)									AS NEW_CHECKING_CURR
			      ,SUM(ifnull(BALANCES.NEW_CHECKING_MTD,0) * TEAM_REF.SPLIT)									AS NEW_CHECKING_MTD
			      ,SUM(ifnull(BALANCES.NEW_CHECKING_QTD,0) * TEAM_REF.SPLIT)									AS NEW_CHECKING_QTD
			      ,SUM(ifnull(BALANCES.NEW_CHECKING_YTD,0) * TEAM_REF.SPLIT)									AS NEW_CHECKING_YTD
			      ,SUM(ifnull(BALANCES.CHECKING_CURR_SINCE_INCEP,0) * TEAM_REF.SPLIT)							AS CHECKING_CURR_SINCE_INCEP
			      ,SUM(ifnull(BALANCES.CHECKING_MTD_SINCE_INCEP,0) * TEAM_REF.SPLIT)							AS CHECKING_MTD_SINCE_INCEP
			      ,SUM(ifnull(BALANCES.CHECKING_QTD_SINCE_INCEP,0) * TEAM_REF.SPLIT)							AS CHECKING_QTD_SINCE_INCEP
			      ,SUM(ifnull(BALANCES.CHECKING_YTD_SINCE_INCEP,0) * TEAM_REF.SPLIT)							AS CHECKING_YTD_SINCE_INCEP
			      ,SUM(ifnull(BALANCES.NEW_DEPOSITS_CURR,0) * TEAM_REF.SPLIT)									AS NEW_DEPOSITS_CURR
			      ,SUM(ifnull(BALANCES.NEW_DEPOSITS_MTD,0) * TEAM_REF.SPLIT)									AS NEW_DEPOSITS_MTD
			      ,SUM(ifnull(BALANCES.NEW_DEPOSITS_QTD,0) * TEAM_REF.SPLIT)									AS NEW_DEPOSITS_QTD
			      ,SUM(ifnull(BALANCES.NEW_DEPOSITS_YTD,0) * TEAM_REF.SPLIT)									AS NEW_DEPOSITS_YTD
			      ,SUM(ifnull(BALANCES.DEPOSITS_CURR_SINCE_INCEP,0) * TEAM_REF.SPLIT)							AS DEPOSITS_CURR_SINCE_INCEP
			      ,SUM(ifnull(BALANCES.DEPOSITS_YTD_SINCE_INCEP,0) * TEAM_REF.SPLIT)							AS DEPOSITS_YTD_SINCE_INCEP
			      ,SUM(ifnull(BALANCES.DEPOSITS_MTD_SINCE_INCEP,0) * TEAM_REF.SPLIT)							AS DEPOSITS_MTD_SINCE_INCEP
			      ,SUM(ifnull(BALANCES.DEPOSITS_QTD_SINCE_INCEP,0) * TEAM_REF.SPLIT)							AS DEPOSITS_QTD_SINCE_INCEP
				  --PWM DEPOSIT REFERRALS--
				  ,SUM(ifnull(NEW_CHECKING_CURR_PWM_REFERRAL,0)	* TEAM_REF.SPLIT)								AS NEW_CHECKING_CURR_PWM_REFERRAL
				  ,SUM(ifnull(NEW_CHECKING_MTD_PWM_REFERRAL,0) * TEAM_REF.SPLIT)								AS NEW_CHECKING_MTD_PWM_REFERRAL
				  ,SUM(ifnull(NEW_CHECKING_QTD_PWM_REFERRAL,0) * TEAM_REF.SPLIT)								AS NEW_CHECKING_QTD_PWM_REFERRAL
				  ,SUM(ifnull(NEW_CHECKING_YTD_PWM_REFERRAL,0) * TEAM_REF.SPLIT)								AS NEW_CHECKING_YTD_PWM_REFERRAL
				  ,SUM(ifnull(CHECKING_CURR_SINCE_INCEP_PWM_REFERRAL,0) * TEAM_REF.SPLIT)						AS CHECKING_CURR_SINCE_INCEP_PWM_REFERRAL
				  ,SUM(ifnull(CHECKING_MTD_SINCE_INCEP_PWM_REFERRAL,0) * TEAM_REF.SPLIT)						AS CHECKING_MTD_SINCE_INCEP_PWM_REFERRAL
				  ,SUM(ifnull(CHECKING_QTD_SINCE_INCEP_PWM_REFERRAL,0) * TEAM_REF.SPLIT)						AS CHECKING_QTD_SINCE_INCEP_PWM_REFERRAL
				  ,SUM(ifnull(CHECKING_YTD_SINCE_INCEP_PWM_REFERRAL,0) * TEAM_REF.SPLIT)						AS CHECKING_YTD_SINCE_INCEP_PWM_REFERRAL
				  ,SUM(ifnull(NEW_DEPOSITS_CURR_PWM_REFERRAL,0) * TEAM_REF.SPLIT)								AS NEW_DEPOSITS_CURR_PWM_REFERRAL
				  ,SUM(ifnull(NEW_DEPOSITS_MTD_PWM_REFERRAL,0) * TEAM_REF.SPLIT)								AS NEW_DEPOSITS_MTD_PWM_REFERRAL
				  ,SUM(ifnull(NEW_DEPOSITS_QTD_PWM_REFERRAL,0) * TEAM_REF.SPLIT)								AS NEW_DEPOSITS_QTD_PWM_REFERRAL
				  ,SUM(ifnull(NEW_DEPOSITS_YTD_PWM_REFERRAL,0) * TEAM_REF.SPLIT)								AS NEW_DEPOSITS_YTD_PWM_REFERRAL
				  ,SUM(ifnull(DEPOSITS_CURR_SINCE_INCEP_PWM_REFERRAL,0) * TEAM_REF.SPLIT)						AS DEPOSITS_CURR_SINCE_INCEP_PWM_REFERRAL
				  ,SUM(ifnull(DEPOSITS_MTD_SINCE_INCEP_PWM_REFERRAL,0) * TEAM_REF.SPLIT)						AS DEPOSITS_MTD_SINCE_INCEP_PWM_REFERRAL
				  ,SUM(ifnull(DEPOSITS_QTD_SINCE_INCEP_PWM_REFERRAL,0) * TEAM_REF.SPLIT)						AS DEPOSITS_QTD_SINCE_INCEP_PWM_REFERRAL
				  ,SUM(ifnull(DEPOSITS_YTD_SINCE_INCEP_PWM_REFERRAL,0) * TEAM_REF.SPLIT) 						AS DEPOSITS_YTD_SINCE_INCEP_PWM_REFERRAL


			      ,SUM(ifnull(BALANCES.PWM_YTD,0) * TEAM_REF.SPLIT)											AS PWM_YTD
			      ,SUM(ifnull(BALANCES.PWM_INCEPTION,0) * TEAM_REF.SPLIT)										AS PWM_INCEPTION
			      ,SUM(ifnull(BALANCES.FUNDED_LOANS_AMT,0) * TEAM_REF.SPLIT)									AS FUNDED_LOANS_AMT
			      ,SUM(ifnull(BALANCES.COUNT_OF_FUNDED_LOANS,0) * TEAM_REF.SPLIT)								AS COUNT_OF_FUNDED_LOANS
			      ,SUM(ifnull(BALANCES.PIPELINE_LOAN_AMT,0) * TEAM_REF.SPLIT)									AS PIPELINE_LOAN_AMT
			      ,SUM(ifnull(BALANCES.COUNT_OF_PIPELINE_LOANS,0) * TEAM_REF.SPLIT)							AS COUNT_OF_PIPELINE_LOANS
				  ,SUM(ifnull(BALANCES.PBO_TOTAL_REFERRAL_BAL,0) * TEAM_REF.SPLIT)								AS PBO_TOTAL_REFERRAL_BAL

				  --FUNDED LOANS 
				  ,SUM(ifnull(BALANCES.FUNDED_LOANS_AMT_NEW_MONEY_MODS,0) * TEAM_REF.SPLIT)						AS FUNDED_LOANS_AMT_NEW_MONEY_MODS
				  ,SUM(ifnull(BALANCES.FUNDED_LOANS_AMT_LOAN_LOG_OTHER,0) * TEAM_REF.SPLIT)						AS FUNDED_LOANS_AMT_LOAN_LOG_OTHER
				  ,ROUND(SUM(ifnull(BALANCES.COUNT_OF_FUNDED_LOANS_NEW_MONEY_MODS,0) * TEAM_REF.SPLIT),0)		AS COUNT_OF_FUNDED_LOANS_NEW_MONEY_MODS
				  ,ROUND(SUM(ifnull(BALANCES.COUNT_OF_FUNDED_LOAN_LOG_OTHER,0) * TEAM_REF.SPLIT),0)				AS COUNT_OF_FUNDED_LOAN_LOG_OTHER	
			 
				  --GROWTH METRICS
				  ,SUM(ifnull(BALANCES.GROWTH_DEP_MONTH,0) * TEAM_REF.SPLIT)									AS GROWTH_DEP_MONTH
				  ,SUM(ifnull(BALANCES.GROWTH_DEP_QUARTER,0) * TEAM_REF.SPLIT)								AS GROWTH_DEP_QUARTER
				  ,SUM(ifnull(BALANCES.GROWTH_LOANS_MONTH,0) * TEAM_REF.SPLIT)								AS GROWTH_LOANS_MONTH
				  ,SUM(ifnull(BALANCES.GROWTH_LOANS_QUARTER,0) * TEAM_REF.SPLIT)								AS GROWTH_LOANS_QUARTER
				  ,SUM(ifnull(BALANCES.GROWTH_PWM_MONTH,0) * TEAM_REF.SPLIT)									AS GROWTH_PWM_MONTH
				  ,SUM(ifnull(BALANCES.GROWTH_PWM_QUARTER,0) * TEAM_REF.SPLIT)								AS GROWTH_PWM_QUARTER

				  ,SUM(ifnull(BALANCES.GROWTH_PWM_YEAR,0) * TEAM_REF.SPLIT)									AS GROWTH_PWM_YEAR
				  ,SUM(ifnull(BALANCES.GROWTH_LOANS_YEAR,0) * TEAM_REF.SPLIT)									AS GROWTH_LOANS_YEAR
				  ,SUM(ifnull(BALANCES.GROWTH_DEP_YEAR,0) * TEAM_REF.SPLIT)									AS GROWTH_DEP_YEAR
				  ,SUM(ifnull(BALANCES.GROWTH_CHECKING_YEAR,0) * TEAM_REF.SPLIT)								AS GROWTH_CHECKING_YEAR
				  ,SUM(ifnull(BALANCES.GROWTH_CHECKING_MONTH,0) * TEAM_REF.SPLIT)								AS GROWTH_CHECKING_MONTH
				  ,SUM(ifnull(BALANCES.GROWTH_CHECKING_QUARTER,0) * TEAM_REF.SPLIT)							AS GROWTH_CHECKING_QUARTER

				  --PB/PBO 470 METRICS
				  ,SUM(ifnull(BALANCES.PBO_470_DEPOSITS_CURR_SINCE_INCEP,0) * TEAM_REF.SPLIT)					AS PBO_470_DEPOSITS_CURR_SINCE_INCEP
				  ,SUM(ifnull(BALANCES.PBO_470_DEPOSITS_MTD_SINCE_INCEP,0) * TEAM_REF.SPLIT) 					AS PBO_470_DEPOSITS_MTD_SINCE_INCEP
				  ,SUM(ifnull(BALANCES.PBO_470_DEPOSITS_QTD_SINCE_INCEP,0) * TEAM_REF.SPLIT) 					AS PBO_470_DEPOSITS_QTD_SINCE_INCEP
				  ,SUM(ifnull(BALANCES.PBO_470_DEPOSITS_YTD_SINCE_INCEP,0) * TEAM_REF.SPLIT) 					AS PBO_470_DEPOSITS_YTD_SINCE_INCEP
				  ,SUM(ifnull(BALANCES.PBO_470_CHECKING_CURR_SINCE_INCEP,0) * TEAM_REF.SPLIT)					AS PBO_470_CHECKING_CURR_SINCE_INCEP
				  ,SUM(ifnull(BALANCES.PBO_470_CHECKING_MTD_SINCE_INCEP,0) * TEAM_REF.SPLIT) 					AS PBO_470_CHECKING_MTD_SINCE_INCEP 
				  ,SUM(ifnull(BALANCES.PBO_470_CHECKING_QTD_SINCE_INCEP,0) * TEAM_REF.SPLIT) 					AS PBO_470_CHECKING_QTD_SINCE_INCEP 
				  ,SUM(ifnull(BALANCES.PBO_470_CHECKING_YTD_SINCE_INCEP,0) * TEAM_REF.SPLIT) 					AS PBO_470_CHECKING_YTD_SINCE_INCEP 
				  ,SUM(ifnull(BALANCES.PBO_470_DEPOSITS_CURR_SINCE_NEW,0) * TEAM_REF.SPLIT)  					AS PBO_470_DEPOSITS_CURR_SINCE_NEW  
				  ,SUM(ifnull(BALANCES.PBO_470_DEPOSITS_MTD_SINCE_NEW,0) * TEAM_REF.SPLIT)	  					AS PBO_470_DEPOSITS_MTD_SINCE_NEW	  
				  ,SUM(ifnull(BALANCES.PBO_470_DEPOSITS_QTD_SINCE_NEW,0) * TEAM_REF.SPLIT)	  					AS PBO_470_DEPOSITS_QTD_SINCE_NEW	  
				  ,SUM(ifnull(BALANCES.PBO_470_DEPOSITS_YTD_SINCE_NEW,0) * TEAM_REF.SPLIT)	  					AS PBO_470_DEPOSITS_YTD_SINCE_NEW	  
				  ,SUM(ifnull(BALANCES.PBO_470_CHECKING_CURR_SINCE_NEW,0) * TEAM_REF.SPLIT)  					AS PBO_470_CHECKING_CURR_SINCE_NEW  
				  ,SUM(ifnull(BALANCES.PBO_470_CHECKING_MTD_SINCE_NEW,0) * TEAM_REF.SPLIT)	  					AS PBO_470_CHECKING_MTD_SINCE_NEW	  
				  ,SUM(ifnull(BALANCES.PBO_470_CHECKING_QTD_SINCE_NEW,0) * TEAM_REF.SPLIT)	  					AS PBO_470_CHECKING_QTD_SINCE_NEW	  
				  ,SUM(ifnull(BALANCES.PBO_470_CHECKING_YTD_SINCE_NEW,0) * TEAM_REF.SPLIT)	  					AS PBO_470_CHECKING_YTD_SINCE_NEW
				  
				  -- (DEPOSIT / LOAN) RATIO 
				  ,NULL																							AS DEP_LOAN_RATIO

				  -- Business Financial Accounts Split
				  ,SUM(ifnull(DEP_NAICS_BRKOUT.BFA_NEW_CHECKING_CURR,0) * TEAM_REF.SPLIT)						AS BFA_NEW_CHECKING_CURR
			      ,SUM(ifnull(DEP_NAICS_BRKOUT.BFA_NEW_CHECKING_MTD,0) * TEAM_REF.SPLIT)						AS BFA_NEW_CHECKING_MTD
			      ,SUM(ifnull(DEP_NAICS_BRKOUT.BFA_NEW_CHECKING_QTD,0) * TEAM_REF.SPLIT)						AS BFA_NEW_CHECKING_QTD
			      ,SUM(ifnull(DEP_NAICS_BRKOUT.BFA_NEW_CHECKING_YTD,0) * TEAM_REF.SPLIT)						AS BFA_NEW_CHECKING_YTD
			      ,SUM(ifnull(DEP_NAICS_BRKOUT.BFA_CHECKING_CURR_SINCE_INCEP,0) * TEAM_REF.SPLIT)				AS BFA_CHECKING_CURR_SINCE_INCEP
			      ,SUM(ifnull(DEP_NAICS_BRKOUT.BFA_CHECKING_MTD_SINCE_INCEP,0) * TEAM_REF.SPLIT)				AS BFA_CHECKING_MTD_SINCE_INCEP
			      ,SUM(ifnull(DEP_NAICS_BRKOUT.BFA_CHECKING_QTD_SINCE_INCEP,0) * TEAM_REF.SPLIT)				AS BFA_CHECKING_QTD_SINCE_INCEP
			      ,SUM(ifnull(DEP_NAICS_BRKOUT.BFA_CHECKING_YTD_SINCE_INCEP,0) * TEAM_REF.SPLIT)				AS BFA_CHECKING_YTD_SINCE_INCEP
			      ,SUM(ifnull(DEP_NAICS_BRKOUT.BFA_NEW_DEPOSITS_CURR,0) * TEAM_REF.SPLIT)						AS BFA_NEW_DEPOSITS_CURR
			      ,SUM(ifnull(DEP_NAICS_BRKOUT.BFA_NEW_DEPOSITS_MTD,0) * TEAM_REF.SPLIT)						AS BFA_NEW_DEPOSITS_MTD
			      ,SUM(ifnull(DEP_NAICS_BRKOUT.BFA_NEW_DEPOSITS_QTD,0) * TEAM_REF.SPLIT)						AS BFA_NEW_DEPOSITS_QTD
			      ,SUM(ifnull(DEP_NAICS_BRKOUT.BFA_NEW_DEPOSITS_YTD,0) * TEAM_REF.SPLIT)						AS BFA_NEW_DEPOSITS_YTD
			      ,SUM(ifnull(DEP_NAICS_BRKOUT.BFA_DEPOSITS_CURR_SINCE_INCEP,0) * TEAM_REF.SPLIT)				AS BFA_DEPOSITS_CURR_SINCE_INCEP
			      ,SUM(ifnull(DEP_NAICS_BRKOUT.BFA_DEPOSITS_YTD_SINCE_INCEP,0) * TEAM_REF.SPLIT)				AS BFA_DEPOSITS_YTD_SINCE_INCEP
			      ,SUM(ifnull(DEP_NAICS_BRKOUT.BFA_DEPOSITS_MTD_SINCE_INCEP,0) * TEAM_REF.SPLIT)				AS BFA_DEPOSITS_MTD_SINCE_INCEP
			      ,SUM(ifnull(DEP_NAICS_BRKOUT.BFA_DEPOSITS_QTD_SINCE_INCEP,0) * TEAM_REF.SPLIT)				AS BFA_DEPOSITS_QTD_SINCE_INCEP
				  
				  ,SUM(ifnull(DEP_NAICS_BRKOUT.NON_BFA_NEW_CHECKING_CURR,0) * TEAM_REF.SPLIT)					AS NON_BFA_NEW_CHECKING_CURR
			      ,SUM(ifnull(DEP_NAICS_BRKOUT.NON_BFA_NEW_CHECKING_MTD,0) * TEAM_REF.SPLIT)					AS NON_BFA_NEW_CHECKING_MTD
			      ,SUM(ifnull(DEP_NAICS_BRKOUT.NON_BFA_NEW_CHECKING_QTD,0) * TEAM_REF.SPLIT)					AS NON_BFA_NEW_CHECKING_QTD
			      ,SUM(ifnull(DEP_NAICS_BRKOUT.NON_BFA_NEW_CHECKING_YTD,0) * TEAM_REF.SPLIT)					AS NON_BFA_NEW_CHECKING_YTD
			      ,SUM(ifnull(DEP_NAICS_BRKOUT.NON_BFA_CHECKING_CURR_SINCE_INCEP,0) * TEAM_REF.SPLIT)			AS NON_BFA_CHECKING_CURR_SINCE_INCEP
			      ,SUM(ifnull(DEP_NAICS_BRKOUT.NON_BFA_CHECKING_MTD_SINCE_INCEP,0) * TEAM_REF.SPLIT)			AS NON_BFA_CHECKING_MTD_SINCE_INCEP
			      ,SUM(ifnull(DEP_NAICS_BRKOUT.NON_BFA_CHECKING_QTD_SINCE_INCEP,0) * TEAM_REF.SPLIT)			AS NON_BFA_CHECKING_QTD_SINCE_INCEP
			      ,SUM(ifnull(DEP_NAICS_BRKOUT.NON_BFA_CHECKING_YTD_SINCE_INCEP,0) * TEAM_REF.SPLIT)			AS NON_BFA_CHECKING_YTD_SINCE_INCEP
			      ,SUM(ifnull(DEP_NAICS_BRKOUT.NON_BFA_NEW_DEPOSITS_CURR,0) * TEAM_REF.SPLIT)					AS NON_BFA_NEW_DEPOSITS_CURR
			      ,SUM(ifnull(DEP_NAICS_BRKOUT.NON_BFA_NEW_DEPOSITS_MTD,0) * TEAM_REF.SPLIT)					AS NON_BFA_NEW_DEPOSITS_MTD
			      ,SUM(ifnull(DEP_NAICS_BRKOUT.NON_BFA_NEW_DEPOSITS_QTD,0) * TEAM_REF.SPLIT)					AS NON_BFA_NEW_DEPOSITS_QTD
			      ,SUM(ifnull(DEP_NAICS_BRKOUT.NON_BFA_NEW_DEPOSITS_YTD,0) * TEAM_REF.SPLIT)					AS NON_BFA_NEW_DEPOSITS_YTD
			      ,SUM(ifnull(DEP_NAICS_BRKOUT.NON_BFA_DEPOSITS_CURR_SINCE_INCEP,0) * TEAM_REF.SPLIT)			AS NON_BFA_DEPOSITS_CURR_SINCE_INCEP
			      ,SUM(ifnull(DEP_NAICS_BRKOUT.NON_BFA_DEPOSITS_YTD_SINCE_INCEP,0) * TEAM_REF.SPLIT)			AS NON_BFA_DEPOSITS_YTD_SINCE_INCEP
			      ,SUM(ifnull(DEP_NAICS_BRKOUT.NON_BFA_DEPOSITS_MTD_SINCE_INCEP,0) * TEAM_REF.SPLIT)			AS NON_BFA_DEPOSITS_MTD_SINCE_INCEP
			      ,SUM(ifnull(DEP_NAICS_BRKOUT.NON_BFA_DEPOSITS_QTD_SINCE_INCEP,0) * TEAM_REF.SPLIT)			AS NON_BFA_DEPOSITS_QTD_SINCE_INCEP


				  -- Loan Details Additional
				  ,SUM(ifnull(LOANS_ADDITIONAL.LOAN_OUTSTANDING_BAL_INCEPTION,0) * TEAM_REF.SPLIT)				AS LOAN_OUTSTANDING_BAL_INCEPTION
				  ,SUM(ifnull(LOANS_ADDITIONAL.LOAN_OUTSTANDING_BAL_YTD,0) * TEAM_REF.SPLIT)					AS LOAN_OUTSTANDING_BAL_YTD
				  ,SUM(ifnull(LOANS_ADDITIONAL.COMMITMENT,0) * TEAM_REF.SPLIT)									AS COMMITMENT


				  -- PWM DEP REFERRALS 5031
				  ,SUM(ifnull(PWM_DEP_REFS_5031.PRINCIPAL_GL_ENDING_BAL,0) * TEAM_REF.SPLIT)                  AS "5031_PRINCIPAL_GL_ENDING_BAL"
				  ,SUM(ifnull(PWM_DEP_REFS_5031.PRINCIPAL_MTD_AVERAGE_BAL,0) * TEAM_REF.SPLIT)				AS "5031_PRINCIPAL_MTD_AVERAGE_BAL"
				  ,SUM(ifnull(PWM_DEP_REFS_5031.PRINCIPAL_QTD_AVERAGE_BAL,0) * TEAM_REF.SPLIT)				AS "5031_PRINCIPAL_QTD_AVERAGE_BAL"
				  ,SUM(ifnull(PWM_DEP_REFS_5031.PRINCIPAL_YTD_AVERAGE_BAL,0) * TEAM_REF.SPLIT)				AS "5031_PRINCIPAL_YTD_AVERAGE_BAL"
				  ,SUM(ifnull(PWM_DEP_REFS_5031.CHECKING_PRINCIPAL_GL_ENDING_BAL,0) * TEAM_REF.SPLIT)			AS "5031_CHECKING_PRINCIPAL_GL_ENDING_BAL"
				  ,SUM(ifnull(PWM_DEP_REFS_5031.CHECKING_PRINCIPAL_MTD_AVERAGE_BAL,0) * TEAM_REF.SPLIT)		AS "5031_CHECKING_PRINCIPAL_MTD_AVERAGE_BAL"
				  ,SUM(ifnull(PWM_DEP_REFS_5031.CHECKING_PRINCIPAL_QTD_AVERAGE_BAL,0) * TEAM_REF.SPLIT)		AS "5031_CHECKING_PRINCIPAL_QTD_AVERAGE_BAL"
				  ,SUM(ifnull(PWM_DEP_REFS_5031.CHECKING_PRINCIPAL_YTD_AVERAGE_BAL,0) * TEAM_REF.SPLIT)		AS "5031_CHECKING_PRINCIPAL_YTD_AVERAGE_BAL"


				  -- LIQUIDITY ADVANTAGE DEPOSITS

				  ,SUM(ifnull(LA_DEPOSITS.ON_BALANCE_YIELD_ADVANTAGE,0) * TEAM_REF.SPLIT)                  AS LA_CURR_ON_BALS
				  ,SUM(ifnull(LA_DEPOSITS.OFF_BALANCE_YIELD_ADVANTAGE,0) * TEAM_REF.SPLIT)				   AS LA_CURR_OFF_BALS

			FROM (	
					SELECT *
					,CASE 
						WHEN OVERRIDEFLAG = 1 THEN overridevalue
						ELSE PERCENTAGE
					END AS SPLIT 
					FROM EagleVision.dbo.T_REF_TEAM_INITIALS
					WHERE 1 = 1
					AND CURRENTFLAG = 1 
				  ) TEAM_REF
			LEFT JOIN EDCI_SANDBOX.REPORTING_GPAVON.TL_METRICS_RM_ROLLUP BALANCES
			ON (TEAM_REF.SHAREDINITIALS = BALANCES.ACCT_OFFICER_CD )

			LEFT JOIN (SELECT * FROM EDCI_SANDBOX.REPORTING_GPAVON.TL_LOANS_BFA_NAICS WHERE PROD_DT = $PROD_DT) AS DEP_NAICS_BRKOUT
			ON(TEAM_REF.SHAREDINITIALS =  DEP_NAICS_BRKOUT.ACCT_OFFICER_CD)

			LEFT JOIN (SELECT * FROM EDCI_SANDBOX.REPORTING_GPAVON.TL_LOANS_DETAILS_ADDITIONAL WHERE PROD_DT = $PROD_DT) AS LOANS_ADDITIONAL
			ON(TEAM_REF.SHAREDINITIALS =  LOANS_ADDITIONAL.OFFICER_CD)

			LEFT JOIN (SELECT * FROM EDCI_SANDBOX.REPORTING_GPAVON.TL_PWM_DEP_REFS_5031  WHERE PROD_DT = $PROD_DT) AS PWM_DEP_REFS_5031
			ON(TEAM_REF.SHAREDINITIALS =  PWM_DEP_REFS_5031.PWM_OFCR_CD)

			LEFT JOIN (SELECT * FROM EDCI_SANDBOX.REPORTING_GPAVON.TL_YIELD_ADV_DEP WHERE PROD_DT = $PROD_DT) AS LA_DEPOSITS
			ON(TEAM_REF.SHAREDINITIALS = LA_DEPOSITS.ACCT_OFFICER_CD)

			LEFT JOIN (SELECT * FROM EagleVision.dbo.V_REF_PEOPLE_INITIALS_CURR WHERE CURRENTFLAG = 1) FRB_PLP
			ON (FRB_PLP.INITIALS = TEAM_REF.INITIALS)

			WHERE 1 = 1
			AND TEAM_REF.CURRENTFLAG = 1 
			AND BALANCES.PROD_DT = $PROD_DT_SELECT

			GROUP BY
			 TEAM_REF.INITIALS
			,TEAM_REF.SHAREDINITIALS
			,TEAM_REF.SPLIT
			,TEAM_REF.NAME
			,FRB_PLP.NAME
			,BALANCES.PROD_DT

			
			) AS RM_TEAMSPLIT_NUMBERS
ON (RM_TEAMSPLIT_NUMBERS.INITIALS_TR = TABLEAU_OUTPUT.INITIALS) 
WHERE 1 = 1
AND TABLEAU_OUTPUT.PROD_DT =  $PROD_DT_SELECT
AND TABLEAU_OUTPUT.SELECTED_OFFICER_LOGIN = $LOGIN

`
   
   
   } );
return 'Tableau Output Splits done.';
$$;


