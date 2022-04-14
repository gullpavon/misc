'''
Gull Pavon: The below simplifies creating tables in Snowflake for the following mediums:
1) Excel -> Snowflake Table
2) SQL Server -> Snowflake Table
3) Pandas Dataframe -> Snowflake Table
4) Snowflake Table -> Snowflake Table New
'''


import pandas as pd
import pyodbc
import snowflake.connector
#conda install -c conda-forge snowflake-sqlalchemy
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy.dialects import registry
import os

os.environ['HTTP_PROXY'] = 'webproxyfrb.iglobal.firstrepublic.com:8080'
os.environ['HTTPS_PROXY'] = 'webproxyfrb.iglobal.firstrepublic.com:8080'

creds = {
    'user': 'gpavon',
    'authenticator': 'externalbrowser',
    'account': 'firstrepublicanalytics',
    'warehouse': 'FRB_WH',
    'database': 'EDCI_SANDBOX',
    'schema': 'REPORTING_GPAVON',
    'role': 'GG_SNOWFLAKE_REPORTING_GPAVON'
}

conn = snowflake.connector.connect(**creds)


registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')

#Snowflake writeback engine
engine = create_engine(URL(
    account = 'firstrepublicanalytics',
    user = 'gpavon',
    authenticator = 'externalbrowser',
    database = 'EDCI_SANDBOX',
    schema = 'REPORTING_GPAVON',
    warehouse = 'FRB_WH',
    role='GG_SNOWFLAKE_REPORTING_GPAVON',
))
 
connection = engine.connect()
 


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



MSSQL_query = '''
SELECT * FROM 
[DataScienceAndAnalytics].[teamlead].[TABLEAU_OUTPUT_SPLITS]
where prod_dt between '2022-03-29' and '2022-03-31'
'''


df = get_data(MSSQL_query)

df.to_sql('tl_tableau_output_splits_history', con=engine, index=False, if_exists='append', chunksize=16000) #make sure index is False, Snowflake doesnt accept indexes
 
connection.close()
engine.dispose()


def SFCreateTable (input, output=Snowflake, outputTableName=, creds=creds):
'''
Input:
1) SQLquery
2) Excel
3) CSV
4) DF (Pandas)
5) Snowflake 

Output: 
1) Snowflake (Default)



'''


'''
from excel file
'''

excel_df = pd.read_excel(r'C:\Users\gpavon\OneDrive - First Republic Bank\Desktop\Projects\Transactions NLP\Preprocessing\EntityRecognition\common_entity_lookup.xlsx', SHEET='Entities')

excel_df = pd.read_csv(r'C:\Users\gpavon\Downloads\companies_sorted.csv (1)\companies_sorted_test.csv',  error_bad_lines=False) #sep='|'


#REMOVE DUPLICATE COLUMN NAMES
excel_df = excel_df[['INPUT_TEXT','CANONICAL_ENTITY','COMPANY_DESC','TYPE_LVL1','TYPE_LVL2']]
excel_df.columns = map(str.upper, excel_df.columns)
excel_df = excel_df.loc[:,~excel_df.columns.duplicated()]
excel_df.fillna("", inplace=True)
excel_df["PROD_DT"] = pd.to_datetime(excel_df["PROD_DT"]).dt.strftime("%m/%d/%Y %H:%M:%S")



excel_df.to_sql('zlookup_company_names_manual', con=engine, index=False, if_exists='replace', chunksize=16000) #make sure index is False, Snowflake doesnt accept indexes
 
connection.close()
engine.dispose()



'''
SNOWFLAKE -> SQL SERVER 

'''
import urllib  # url string manipulations
import sqlalchemy

import requests
import snowflake.connector
import pandas as pd
import os
import time
from pandas.tseries.offsets import MonthEnd
from datetime import date, timedelta, datetime
import numpy as np
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy.dialects import registry
#conda install -c conda-forge snowflake-sqlalchemy
registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')
'''Start Timer - Shows how long this process takes to run.'''
start = time.time()
os.environ['HTTP_PROXY'] = 'webproxyfrb.iglobal.firstrepublic.com:8080'
os.environ['HTTPS_PROXY'] = 'webproxyfrb.iglobal.firstrepublic.com:8080'
creds = {
    'user': 'gpavon',
    'authenticator': 'externalbrowser',
    'account': 'firstrepublicanalytics',
    'warehouse': 'FRB_WH',
    'database': 'EDCI_SANDBOX',
    'schema': 'REPORTING_GPAVON',
    'role': 'GG_SNOWFLAKE_REPORTING_GPAVON'
}

conn = snowflake.connector.connect(**creds)

snowflake_sql_query = """
select top 0 * from EDCI_SANDBOX.REPORTING_GPAVON.BP_T_CLIENT_INFO_CURR
"""


curs=conn.cursor()
curs.execute(snowflake_sql_query)

df= pd.DataFrame.from_records(iter(curs), columns=[x[0] for x in curs.description])









def get_default_server():
    '''
    Return the environment-dependent default SQL server.
    If you don't know what server to use, this function
    will give you a suitable choice.
    
    Production Windows: VDBEDCISANDBOX
    Quant Windows: DC1QEDCISQLEEV
    Quant Linux: DC1QEDCISQLEEV
    '''
    
    if os.name == 'posix':
        DEFAULT_SERVER = "DC1QEDCISQLEEV"
    elif os.name == 'nt':
        if os.path.exists("Q:"):
            DEFAULT_SERVER = "DC1QEDCISQLEEV"
        else:
            DEFAULT_SERVER = "VDBEDCISANDBOX"
    else:
        raise ValueError('Unknown system')

    return DEFAULT_SERVER

def get_conn_str(server=None, database=None, user=None, password=None, use_fqdn=True):
    """
    Produce the formatted connection string consumed by pyodbc/sqlchemy.
    Note the environment-dependent SQL driver is determined automatically.
    
        server - name of server (optional).
        database - name of database (optional).
        user - user name (optional). Not needed if using Trusted Connection.
        password - user password (optional). Not needed if using Trusted Connection.
        use_fqdn - True/False use server's fully qualified domain name.
    """
    
    # server
    if server is None:
        server = get_default_server()

    # fqdn
    if (server.find('.corp.firstrepublic.com') == -1) & use_fqdn==True:
        server += '.corp.firstrepublic.com'  # use full FQDN of server

    # Choose driver options depending on the environment
    if os.name == "nt":  # Windows
        driver = "{ODBC Driver 13 for SQL Server}"
        trusted_conn = "Trusted_Connection=yes"

    else:  # Not windows
        driver = "{ODBC Driver 13 for SQL Server}"
        trusted_conn = "Trusted_Connection=yes"

    # user name and password
    if user and password:
        trusted_conn = "UID={uid};PWD={pwd}".format(uid=user, pwd=password)

    # assemble the connection string
    driver_info = "DRIVER={}".format(driver)
    server_info = "SERVER={}".format(server)
    db_info = "DATABASE={}".format(database) if database else ""

    conn_info = [driver_info, server_info, db_info, trusted_conn]
    conn_info = [info for info in conn_info if info]  # drop NULL values
    conn_str = ";".join(conn_info)
    
    return conn_str

def get_alchemy_engine(server=None, database=None, user=None, password=None,
                       use_fqdn=True, fast_executemany=True):
    '''
    Generate SQL Alchemy engine for reading or writing SQL.  If writing,
    ensure fast_executemany=True (the default) to speed up writing.
    '''

    conn_str = get_conn_str(server=server, database=database, user=user,
                            password=password, use_fqdn=use_fqdn)

    conn_url = urllib.parse.quote_plus(conn_str)
    engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}"
                                      .format(conn_url),
                                      fast_executemany=fast_executemany) 
    return engine

write_conn = get_alchemy_engine(database='Reporting_GPavon')  # use the default server


# upload as a new table
import timeit
start = timeit.timeit()
df.to_sql('BP_T_CLIENT_INFO_CURR_TODAY', write_conn, schema='dbo', if_exists='replace', index=False, chunksize=None, method='multi')
end = timeit.timeit()
print(end - start)