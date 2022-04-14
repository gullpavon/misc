USE ROLE GG_SNOWFLAKE_REPORTING_GPAVON;
USE WAREHOUSE FRB_WH;
USE DATABASE EDCI_SANDBOX;
USE SCHEMA REPORTING_GPAVON;

CREATE or replace PROCEDURE SP_TL_START()
RETURNS VARCHAR
LANGUAGE javascript
EXECUTE AS CALLER
AS
$$

let CurrDate = new Date()
CurrDate.toISOString().split('T')[0]
const offset = CurrDate.getTimezoneOffset()
CurrDate = new Date(CurrDate.getTime() - (offset*60*1000))
tminus1 = CurrDate.setDate(CurrDate.getDate() - 1);


var report_date = CurrDate.toISOString().split('T')[0];                                   //'2022-01-31'
var date_key =  CurrDate.toISOString().split('T')[0].replace('-','').replace('-','');    // '20220131'

///If data exists, delete from staging tables.	This bulletproofs so that data cannot be loaded twice and consequently inhibits duplication issues
var rs_delete_frb_org = snowflake.execute( { sqlText: `DELETE FROM "EDCI_SANDBOX"."REPORTING_GPAVON"."TL_FRB_ORG" WHERE insert_dt = current_date;`});
var rs_delete_hierarchy = snowflake.execute( { sqlText: `DELETE FROM "EDCI_SANDBOX"."REPORTING_GPAVON"."TL_HIERARCHY" WHERE PROD_DT = '${report_date}';`});
var rs_delete_loan_details_additional = snowflake.execute( { sqlText: `DELETE FROM "EDCI_SANDBOX"."REPORTING_GPAVON"."TL_LOANS_DETAILS_ADDITIONAL" WHERE PROD_DT = '${report_date}';`});
var rs_delete_product_segments = snowflake.execute( { sqlText: `DELETE FROM "EDCI_SANDBOX"."REPORTING_GPAVON"."TL_PRODUCT_SEGMENTS" WHERE PROD_DT = '${report_date}';`});
var rs_delete_pwm_dep_ref = snowflake.execute( { sqlText: `DELETE FROM "EDCI_SANDBOX"."REPORTING_GPAVON"."TL_PWM_DEP_REFS_5031" WHERE PROD_DT = '${report_date}';`});
var rs_delete_ya_dep = snowflake.execute( { sqlText: `DELETE FROM "EDCI_SANDBOX"."REPORTING_GPAVON"."TL_YIELD_ADV_DEP" WHERE PROD_DT = '${report_date}';`});
var rs_delete_bfa_naics_dep = snowflake.execute( { sqlText: `DELETE FROM "EDCI_SANDBOX"."REPORTING_GPAVON"."TL_LOANS_BFA_NAICS" WHERE PROD_DT = '${report_date}';`});
var rs_delete_tl_metrics = snowflake.execute( { sqlText: `DELETE FROM "EDCI_SANDBOX"."REPORTING_GPAVON"."TL_METRICS_RM_ROLLUP" WHERE PROD_DT = '${report_date}';`});
var rs_delete_tl_tableau_output = snowflake.execute( { sqlText: `DELETE FROM "EDCI_SANDBOX"."REPORTING_GPAVON"."TL_TABLEAU_OUTPUT" WHERE PROD_DT = '${report_date}';`});
var rs_delete_tl_tableau_output_splits = snowflake.execute( { sqlText: `DELETE FROM "EDCI_SANDBOX"."REPORTING_GPAVON"."TL_TABLEAU_OUTPUT_SPLITS" WHERE PROD_DT = '${report_date}';`});


//add tableau output splits


//Start creating data needed
var rs_frb_org = snowflake.execute( { sqlText: `CALL SP_FRB_ORG(TO_VARCHAR(SUBSTRING('${report_date}',1,10)));`});
var rs_loan_details_additional = snowflake.execute( { sqlText: `CALL SP_TL_LOANS_ADDITIONAL(TO_VARCHAR(SUBSTRING('${report_date}',1,10)));`});
var rs_product_segments = snowflake.execute( { sqlText: `CALL SP_TL_PRODUCT_SEGMENTS(TO_VARCHAR(SUBSTRING('${report_date}',1,10)));`});
var rs_pwm_dep_ref_p1 = snowflake.execute( { sqlText: `CALL SP_TL_PWM_DEP_REFS_5031('${date_key}');`});
var rs_pwm_dep_ref_p2 = snowflake.execute( { sqlText: `CALL SP_TL_PWM_DEP_REFS_5031_P2('${date_key}');`});
var rs_yield_adv_dep =  snowflake.execute( { sqlText: `CALL SP_TL_YA_DEP('${date_key}');`}); 
var rs_bfa_naics_dep =  snowflake.execute( { sqlText: `CALL SP_TL_BFA_NAICS('${date_key}');`}); 
var rs_tl_metrics = snowflake.execute( { sqlText: `CALL SP_TL_METRICS(TO_VARCHAR(SUBSTRING('${report_date}',1,10)));`});


//Array created to house users that we will be producing TL2.0 reports for. 
var report_users = [];

// create Teamleader 2.0 reports for the following users
var rs = snowflake.execute( { sqlText: 
`
SELECT 
DISTINCT CAST(FRB_ORG.PAYEEID AS INT) as PAYEEID

	FROM EagleVision.dbo.T_REF_PEOPLE_INITIALS FRB_PLP -- [EagleVision].[dbo].[V_REF_PEOPLE_INITIALS_CURR]
	LEFT JOIN  "EDCI_SANDBOX"."REPORTING_GPAVON"."TL_FEATURES_NGRM_PEOPLE" NGRM_PLP
	ON (FRB_PLP.INITIALS = NGRM_PLP.INITIALS)
	INNER JOIN "EDCI_SANDBOX"."REPORTING_GPAVON"."TL_FRB_ORG" FRB_ORG
	ON (FRB_ORG.INITIALS = FRB_PLP.INITIALS)
	WHERE 1 = 1 
	AND FRB_PLP.CURRENTFLAG = 1 
	AND (FRB_PLP.EMPLOYMENTSTATUS in ('ACTIVE','Leave of absence','Leave') OR NGRM_PLP.ACTIVE = 'Y')
	/*BELOW IS THE LIST OF SUBJECTS THAT WE PROVIDE SUMMARIES FOR.*/
	AND FRB_ORG.PayeeID IN 
							(	  
							      '10748'	  -- Castro Franceschi, Carmen H.
                                  ,'10818'	  -- Abreu, Kellie J.
                                  ,'13514'	  -- Added Alex Mcdougall in replacement of Jacob -- 1/3/2022
								  ,'14291'	  -- Meany, James B.
								  ,'10942'    -- Hirano, Anna -- added 8/24/2020
								  ,'10471'	  -- Hutchinson, Kimberley R.
								  ,'11260'	  -- Harkins, Julie B.
								  ,'10884'	  -- Thornton, Bob -- added 8/25/2020
								  ,'10579'	  -- Tresenfeld, Dyann
								  ,'13568'	  -- Palmer, Barbara D.
								  ,'11105'    -- Patrick Macken -- added 6/15/2020
								  ,'10014'	  -- Kasaris, Mary
								  ,'10253'    --- mike franks -- added 1/13/22, replaces mary kasaris     
								  ,'14330'	  -- Sattar, Shiva
								  ,'10425'    -- Scott Dufresne -- added 6/15/2020
								  ,'10522'	  -- Dessoffy, William G.
								  ,'10264'	  -- Deckebach, Mary C.
								  ,'12065'	  -- Rassiger, Todd C.
								  ,'10975'	  -- Wen, Elise C.
								  ,'11232'	  -- Coleman, Christopher B.
								  ,'10808'	  -- Mak, Margaret
								  ,'10252'	  -- Morris, Nyree S. -- ADDED 6/20/2020
								  ,'13015'	  -- Selfridge, Michael D.
								  ,'11700'	  -- Stevens, Amie M. -- ADDED 4/7/2020
								  ,'10788'    -- Jim Herbert / ALL RMs output
								  ,'19808'    -- James Herbert (The son) - ADDED 6/19/2020
								  ,'11007'	  -- Mohamed Fahmi -- ADDED 5/20/2020
								  , '1'		  -- COHORT 1
								  , '2'		  -- COHORT 2
								  , '3'		  -- COHORT 3
								  , '4'		  -- COHORT 4
								  , '5'		  -- COHORT 5
								  , '20'      -- ALL COHORTS
								  , '23199'   -- Brian Moroz
                                  
                                  )
`} );

//load values from table into Array (similar to Python's list),  we will be looping through the arrays to execute the store procs that will run the etl process 
while (rs.next()){
    var report_user_id = rs.getColumnValue(1);
    report_users.push(report_user_id);
                  }

for (var i = 0; i < report_users.length; i++) {

        snowflake.execute( { sqlText: `CALL SP_TL_HIERARCHY(?, SUBSTRING(TO_VARCHAR('${report_date}'), 1, 10)) ;`, binds: [report_users[i]]});
        
        
             };
              
             
//---------  RUN TABLEAU OUTPUT CREATE 

var report_logins = [];

var rs_logins = snowflake.execute( { sqlText: 
`

SELECT 
	DISTINCT SELECTED_OFFICER_LOGIN
	FROM EDCI_SANDBOX.REPORTING_GPAVON.TL_HIERARCHY
	WHERE 1 = 1 
	--AND PROD_DT = @PROD_DT_SELECT
	AND SELECTED_OFFICER_LOGIN IN 
							(	  
								  'ccastro'
								 ,'ccolema1'
								 ,'JHerbert'
								 ,'jaherbert'
								 ,'KHutchinson'
								 ,'Kabreu'
								 ,'JMeany'
								 ,'jharkins'
								 ,'Dtresenfeld'
								 ,'ewen'
								 ,'Astevens'
								 ,'AHui'
								 ,'bpalmer'
								 ,'Pmacken'
								 ,'mselfridge'
								-- ,'jlamarine'
							     ,'amcdougall' -- replaces jlamarine, 1/3/2022
								 ,'Mdeckebach'
								 ,'Mfahmi'
								 ,'MKasaris'
								 ,'MMak'
								 ,'Rthornton'
								 ,'ssattar'
								 ,'Sdufresne'
								 ,'trassiger'
								 ,'BDessoffy'
								 ,'cohort 1'
								 ,'cohort 2'
								 ,'cohort 3'
								 ,'cohort 4'
								 ,'cohort 5'
								 ,'cohort all'
								 ,'nmorris' -- added 6/20/2020    	 
								 ,'bmoroz'   -- Brian Moroz added  1/3/22 
								 ,'mifranks'  -- added 1/13/22, replaces mary kasaris
							  

							)



`} );

while (rs_logins.next()){
    var report_user_login = rs_logins.getColumnValue(1);
    report_logins.push(report_user_login);
                  }

for (var i = 0; i < report_logins.length; i++) {

        snowflake.execute( { sqlText: `CALL SP_TL_TABLEAU_OUTPUT(?, SUBSTRING(TO_VARCHAR('${report_date}'), 1, 10)) ;`, binds: [report_logins[i]]});
        snowflake.execute( { sqlText: `CALL SP_TL_TABLEAU_OUTPUT_SPLITS(?, SUBSTRING(TO_VARCHAR('${report_date}'), 1, 10)) ;`, binds: [report_logins[i]]});
        
        
             };




var rs_delete_cohort_dupes = snowflake.execute( { sqlText: `

DELETE FROM "EDCI_SANDBOX"."REPORTING_GPAVON"."TL_TABLEAU_OUTPUT" 
WHERE PROD_DT = '${report_date}' 
AND SELECTED_OFFICER_INITIALS IS NOT NULL 
AND SELECTED_OFFICER_NAME IN ('COHORT 1', 'COHORT 2','COHORT 3','COHORT 4', 'COHORT 5') ;

`});
    
             
             
             
             
             
$$;
  
  
CALL SP_TL_START ()
