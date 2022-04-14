'''
The purpose of this module is to extract payroll correctly. 

We have accounts that can have more than one client attached to, when payrolls come in we need to deduce
who the payroll is actually going to. 

This will help create the payroll flag in the RFM model. 


'''


import os.path, sys
#below allows me to import helpers
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),os.pardir))

from helpers import utils
from data import data
import pandas as pd
import numpy as np
from pyjarowinkler import distance
import pyodbc
#Load all payroll & client info




def get_conn():
    """
    Get a connection to the EDCI database from Microsoft SQL Server.
    Returns
    -------
    conn : pyodbc.Connection
        A connection object to the EDCI database.
    """

    server = "VDBEDCISANDBOX;"
    odbc_conn = "Driver={ODBC Driver 13 for SQL Server};SERVER=" + server + ";Trusted_Connection=yes"
    return pyodbc.connect(odbc_conn, autocommit=True)


def get_data(data):
    df = pd.read_sql(data, get_conn())
    return df


payroll_and_clients = """


DECLARE @DATE DATE = DATEADD(MM, -2, GETDATE());

/*GET ALL PRIMARY AND SECONDARIES FOR ACCOUNTS*/
WITH ALL_CUST_DATA AS (


		SELECT 
		  A.ACCT_NBR
		, A.SOURCE_CD
		, C.CDM_CUST_KEY
		, REPLACE(C.CUST_NM,'	',' ') AS CUST_NM
		,'Primary' REL_CD
		, C.PRIME_ADDR_STREET
		, C.PRIME_ADDR_STREET2
		, C.PRIME_ADDR_CITY
		, C.PRIME_ADDR_STATE
		, C.PRIME_ADDR_ZIP
		, C.PERSON_ORG_FLG
		FROM CUSTDM..T_CDM_ACCT AS A 
		
		LEFT JOIN CUSTDM..T_CDM_ACCT_FACT_CURR AS F 
			ON A.CDM_ACCT_KEY = F.CDM_ACCT_KEY
		LEFT JOIN CUSTDM..T_CDM_CUST AS C 
			ON F.CDM_CUST_KEY = C.CDM_CUST_KEY
		
		WHERE 1 = 1
		AND A.CURR_REC_IND = 1
		--AND A.ACCT_STAT_CD = 1
		AND C.CURR_REC_IND = 1	
		
		UNION ALL 
		-- SECONDARIES
		SELECT 
		  A.ACCT_NBR
		, A.SOURCE_CD
		, AC.CDM_CUST_KEY ---
		, REPLACE(AC.CUST_NM,'	',' ') AS CUST_NM
		, RC.REL_CD_DESC
		, C.PRIME_ADDR_STREET
		, C.PRIME_ADDR_STREET2
		, C.PRIME_ADDR_CITY
		, C.PRIME_ADDR_STATE
		, C.PRIME_ADDR_ZIP
		, C.PERSON_ORG_FLG
		--INTO [DataScienceAndAnalytics].[BP].[Custs] 
		FROM CUSTDM..T_CDM_ACCT AS A 
		
		LEFT JOIN CUSTDM..T_CDM_ACCT_CUST AS AC 
			ON (A.CDM_ACCT_KEY = AC.CDM_ACCT_KEY)
		
		LEFT JOIN CUSTDM..T_CDM_ACCT_FACT_CURR AS F 
			ON A.CDM_ACCT_KEY = F.CDM_ACCT_KEY
		LEFT JOIN CUSTDM..T_CDM_CUST AS C 
			ON F.CDM_CUST_KEY = C.CDM_CUST_KEY
		
		LEFT JOIN CUSTDM..T_CDM_REL_CD AS RC 
			ON AC.CDM_REL_CD = RC.REL_CD
		
		WHERE 1 = 1
		AND A.CURR_REC_IND = 1
		--AND A.ACCT_STAT_CD = 1
		AND C.CURR_REC_IND = 1
		
			)
,



/* Get payroll transactions for MCKINSEY employees */
PAYROLL_TX AS 

(
SELECT
 REPLACE(SUBSTRING (STUFF(DFI_ACCT_NBER, 1, PATINDEX('%[0-9]%', DFI_ACCT_NBER)-1, ''),PATINDEX('%[^0]%', STUFF(DFI_ACCT_NBER, 1, PATINDEX('%[0-9]%', DFI_ACCT_NBER)-1, ''))
	,LEN(STUFF(DFI_ACCT_NBER, 1, PATINDEX('%[0-9]%', DFI_ACCT_NBER)-1, ''))), '-', '') AS ACCT_NBR
,CONVERT(DATE, [PROD_DT]) AS PROD_DT
,[DFI_ACCT_NBER]
,[TRACE_NBR]
,[COMPANY_ENTRY_DESC]
,LTRIM(RTRIM(UPPER([COMPANY_NM]))) AS COMPANY_NM
,LTRIM(RTRIM(UPPER([INDIVID_NM]))) AS INDIVID_NM
, CONVERT(DATE, SETTLE_DT) AS SETTLE_DT
, RDFI_AMT AS AMT

FROM
EAGLEVISION.DBO.T_TVN_ACH_ACTIVITY A
WHERE 1=1

AND
(
          COMPANY_ENTRY_DESC LIKE '%payroll%'  
                     OR COMPANY_ENTRY_DESC LIKE '%direct%'
                     OR COMPANY_ENTRY_DESC LIKE '%payroll%'  
                     OR COMPANY_ENTRY_DESC LIKE '%PAYRLL%'
                     OR COMPANY_ENTRY_DESC LIKE '%PAYRL%'
                     OR COMPANY_ENTRY_DESC LIKE '%direct%' 
                     OR COMPANY_ENTRY_DESC LIKE '%deposit%'
                           --AND COMPANY_ENTRY_DESC  not like '%DIR DEP%'
                     OR COMPANY_ENTRY_DESC LIKE '%DEP %' 
                     OR COMPANY_ENTRY_DESC LIKE '%DIR %' 
                     OR COMPANY_ENTRY_DESC LIKE '%dirdep%'
                     OR COMPANY_ENTRY_DESC LIKE '%NET PAY%'
                     OR COMPANY_ENTRY_DESC LIKE '%NETPAY%'
                           -- INTER BUS MACH  -IBMNETPAYS           -- IBM PAYROLL
                     OR COMPANY_ENTRY_DESC LIKE '%SALARY%'
                     OR COMPANY_ENTRY_DESC LIKE '%reg %'
                     OR COMPANY_ENTRY_DESC LIKE '%DIR DEP%'
                     OR COMPANY_ENTRY_DESC LIKE '%PAYRLL%'
                     OR COMPANY_ENTRY_DESC LIKE '%PAYRL%'
                     OR COMPANY_ENTRY_DESC LIKE '%EARNINGS%'
                     OR COMPANY_ENTRY_DESC LIKE '%BATCH%'
                     OR COMPANY_ENTRY_DESC LIKE '%PR PAYMENT%'
					 OR COMPANY_ENTRY_DESC LIKE '%PYMENTS%'
                     OR COMPANY_ENTRY_DESC LIKE '%DIRDEP%'
                     OR COMPANY_ENTRY_DESC = 'FED SAL'
                     OR COMPANY_ENTRY_DESC = 'ADP NJPAYR'
					 OR COMPANY_ENTRY_DESC = 'EDIPAYMENT' -- THEY WAY MICROSOFT PAYS CLIENTS
)

--AND (COMPANY_ENTRY_DESC LIKE '%payroll%'  or COMPANY_ENTRY_DESC LIKE '%direct%' or COMPANY_ENTRY_DESC LIKE '%PAYRLL%' or COMPANY_ENTRY_DESC LIKE '%DIR%' 
--OR COMPANY_ENTRY_DESC LIKE '%DEP%'  or COMPANY_ENTRY_DESC LIKE '%deposit%' or COMPANY_ENTRY_DESC LIKE '%NET%PAY%' or COMPANY_ENTRY_DESC LIKE '%salary%' ) --key words need work
AND SETTLE_DT > @DATE --Receiving payroll in last month

AND REPORT_NBR IN ('00','01') ---Only include completed transations, 00 means completed, 01 means maybe
AND STAND_ENTRY_CLASS_CD = 'PPD' --Prearranged Payment and Deposit Entry
AND [TRAN_CD] IN (21,22,23,24   ,31,32,33,34   ,51,52,53,54) -- CREDIT transactions
----- New requirements:
--AND [RDFI_AMT] > 0
AND RDFI_AMT >= 100

AND LTRIM(RTRIM(UPPER([COMPANY_NM]))) LIKE '%AMAZON%' 
)








SELECT  
 PAYROLL_TX.*
,CUSTOMERS.CDM_CUST_KEY_1
,CUSTOMERS.CUST_NM_1
,CUSTOMERS.CDM_CUST_KEY_2
,CUSTOMERS.CUST_NM_2
,CUSTOMERS.CDM_CUST_KEY_3
,CUSTOMERS.CUST_NM_3
,CUSTOMERS.CDM_CUST_KEY_4
,CUSTOMERS.CUST_NM_4
,CUSTOMERS.CDM_CUST_KEY_5
,CUSTOMERS.CUST_NM_5

FROM PAYROLL_TX

LEFT JOIN
	(
			SELECT 
			ACCT_NBR
			,MAX(CASE WHEN CUSTOMERS.RN = 1 THEN CUSTOMERS.CDM_CUST_KEY END) AS CDM_CUST_KEY_1
			,MAX(CASE WHEN CUSTOMERS.RN = 1 THEN CUSTOMERS.CUST_NM END) AS CUST_NM_1
			,MAX(CASE WHEN CUSTOMERS.RN = 2 THEN CUSTOMERS.CDM_CUST_KEY END) AS CDM_CUST_KEY_2
			,MAX(CASE WHEN CUSTOMERS.RN = 2 THEN CUSTOMERS.CUST_NM END) AS CUST_NM_2
			,MAX(CASE WHEN CUSTOMERS.RN = 3 THEN CUSTOMERS.CDM_CUST_KEY END) AS CDM_CUST_KEY_3
			,MAX(CASE WHEN CUSTOMERS.RN = 3 THEN CUSTOMERS.CUST_NM END) AS CUST_NM_3
			,MAX(CASE WHEN CUSTOMERS.RN = 4 THEN CUSTOMERS.CDM_CUST_KEY END) AS CDM_CUST_KEY_4
			,MAX(CASE WHEN CUSTOMERS.RN = 4 THEN CUSTOMERS.CUST_NM END) AS CUST_NM_4
			,MAX(CASE WHEN CUSTOMERS.RN = 5 THEN CUSTOMERS.CDM_CUST_KEY END) AS CDM_CUST_KEY_5
			,MAX(CASE WHEN CUSTOMERS.RN = 5 THEN CUSTOMERS.CUST_NM END) AS CUST_NM_5
			
			
			FROM
				(
					SELECT 
					 ACCT_NBR
					,CUST_NM
					,CDM_CUST_KEY
					,ROW_NUMBER() OVER(PARTITION BY ACCT_NBR ORDER BY CASE 
																		WHEN REL_CD LIKE '%PRIMARY%'		THEN 1
																		WHEN REL_CD LIKE '%SECONDARY%'		THEN 2
																		WHEN REL_CD LIKE '%OWNER%'			THEN 3
																		WHEN REL_CD LIKE '%BENEFICIARY%'	THEN 4
																		WHEN REL_CD LIKE '%TRUSTEE%'		THEN 5
																		WHEN REL_CD LIKE '%SPOUSE%'			THEN 6
																		WHEN REL_CD LIKE '%GUARANTOR%'		THEN 7
																		WHEN REL_CD LIKE '%CUSTODIAN%'		THEN 8
																		WHEN REL_CD LIKE '%ATTORNEY%'		THEN 9
																		WHEN REL_CD LIKE '%AUTHORIZED%'		THEN 10
																		WHEN REL_CD LIKE '%CHILD%'			THEN 11
																		ELSE 12
																	 END) RN
					FROM ALL_CUST_DATA
					WHERE 1 = 1 
				
				 ) CUSTOMERS
			 
			WHERE 1 = 1 
			GROUP BY ACCT_NBR

	) CUSTOMERS
ON (PAYROLL_TX.ACCT_NBR = CUSTOMERS.ACCT_NBR) 
WHERE 1 = 1



"""    

payrollnClients = get_data(payroll_and_clients)


#Reverse names from ACH tables. i.e. Doe, John = John Doe, this helps with jaro later on when identify who the payroll is going to on the account
payrollnClients['INDIVID_NM_CLEAN'] = payrollnClients['INDIVID_NM'].apply(lambda x: ' '.join(x.split(',')[::-1]).strip())


#Apply Jaro on payroll name and all five client names. Pick the client with the highest score. 
payrollnClients['J1'] = payrollnClients.apply(lambda d: -1 if not str(d['CUST_NM_1']) or not str(d['INDIVID_NM_CLEAN']) or d['CUST_NM_1'] == None else distance.get_jaro_distance(str(d['CUST_NM_1']),str(d['INDIVID_NM_CLEAN']),winkler=True,scaling=0.1), axis=1)
payrollnClients['J2'] = payrollnClients.apply(lambda d: -1 if not str(d['CUST_NM_2']) or not str(d['INDIVID_NM_CLEAN']) or d['CUST_NM_2'] == None else distance.get_jaro_distance(str(d['CUST_NM_2']),str(d['INDIVID_NM_CLEAN']),winkler=True,scaling=0.1), axis=1)
payrollnClients['J3'] = payrollnClients.apply(lambda d: -1 if not str(d['CUST_NM_3']) or not str(d['INDIVID_NM_CLEAN']) or d['CUST_NM_3'] == None else distance.get_jaro_distance(str(d['CUST_NM_3']),str(d['INDIVID_NM_CLEAN']),winkler=True,scaling=0.1), axis=1)
payrollnClients['J4'] = payrollnClients.apply(lambda d: -1 if not str(d['CUST_NM_4']) or not str(d['INDIVID_NM_CLEAN']) or d['CUST_NM_4'] == None else distance.get_jaro_distance(str(d['CUST_NM_4']),str(d['INDIVID_NM_CLEAN']),winkler=True,scaling=0.1), axis=1)
payrollnClients['J5'] = payrollnClients.apply(lambda d: -1 if not str(d['CUST_NM_5']) or not str(d['INDIVID_NM_CLEAN']) or d['CUST_NM_5'] == None else distance.get_jaro_distance(str(d['CUST_NM_5']),str(d['INDIVID_NM_CLEAN']),winkler=True,scaling=0.1), axis=1)


#For each row grab the cdm_cust_key (out of the possible 5 clients we look at) that best matches with jaro

idx_keys = 'CDM_CUST_KEY_' + payrollnClients[['J1','J2','J3','J4','J5']].idxmax(axis=1).str[1]

payrollnClients['PAYROLL_CLIENT'] = [payrollnClients.loc[idx,key] for idx,key in idx_keys.iteritems()]




#output to excel
payrollnClients.to_excel(r'\\corp.firstrepublic.com\userstate\gpavon\Desktop\Projects\Sea Eagle\payroll_extraction\payroll_client_data_amazon.xlsx', index = True)


payrollnClients.head(15)
pd.set_option('display.max_columns', None)


#grab the month and the payroll client, you will use this to join back the main df. 
payrollDF = payrollnClients[['PROD_DT','DFI_ACCT_NBER','PAYROLL_CLIENT',]]


#create a function so i can call the df in the main file

def payrolldf():
    return payrollDF