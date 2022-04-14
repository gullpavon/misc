
from helpers import utils
from data import data

import pandas as pd
import numpy as np
import scipy.stats as stats
import seaborn as sns
import matplotlib.pyplot as plt

from datetime import date, timedelta, datetime
from pandas.tseries.offsets import MonthEnd

import pyodbc
def get_conn():
    """
    Get a connection to the EDCI database.
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




dimensions = '''
SELECT 
	   [CDM_CUST_KEY]
      ,[PREV_CLIENT_DEPOSIT_COMMITMENT]
      ,[DEPOSIT_COMMITMENT]
      ,[LOAN_APP_POST_LOAN_LIQUIDITY]
      ,[LOAN_APP_NET_WORTH]
      ,[LOAN_APP_FICO]
      ,[LOAN_APP_DEBT_TO_INCOME]
      ,[LOAN_APP_INCOME1040]
      ,[LOAN_APP_TOTAL_INCOME]
      ,[LOAN_APP_ASSETS_CASH]
      ,[LOAN_APP_ASSETS_SECURITIES]
      ,[LOAN_APP_ASSETS_RETIREMENT]
      ,[LOAN_APP_ASSETS_TRUST]
      ,[LOAN_APP_ASSETS_OTHER]
      ,[TRANSACTION_COUNT]
      ,[TRANSACTION_ATM_ACTIVE]
      ,[TRANSACTION_GROSS_AMT]
      ,[TRANSACTION_OUTGOING_ACH]
      ,[TRANSACTION_OUTGOING_WIRE]

      ,[TRANSACTION_DD_IS_CURRENT]
      ,[TRANSACTION_DD]
      ,[TRANSACTION_PAYROLL_IS_CURRENT]
      ,[TRANSACTION_PAYROLL]
      ,[IS_HEAD_OF_HOUSEHOLD]
      ,[HH_MEMBERS]
      ,[HH_MEMBERS_WITH_OPEN_PRIMARY_ACCT]
      ,[HH_DEPOSIT_TO_LOAN_RATIO]
      ,[CD_ONLY]
      ,[HAS_CHECKING]
      ,[HAS_SFR]
      ,[HAS_HDR]
      ,[CHECKING]
      ,[ICS]
      ,[DEPOSITS]
      ,[DRAWN]
      ,[COMMITMENT]
      ,[AUM]
      ,[HH_CD_ONLY]
      ,[HH_HAS_CHECKING]
      ,[HH_HAS_SFR]
      ,[HH_HAS_HDR]
      ,[HH_CHECKING]
      ,[HH_ICS]
      ,[HH_DEPOSITS]
      ,[HH_DRAWN]
      ,[HH_COMMITMENT]
      ,[HH_AUM]

      ,[COMMITMENT_DD]
      ,[COMMITMENT_BP]
      ,[CHECKING_12M]
      ,[HH_CHECKING_12M]
      ,case when [HAS_ATM_CARD] = 'y' then 1 else 0 end [HAS_ATM_CARD]
      ,[HH_TRANSACTION_ATM_ACTIVE]
      ,[HH_TRANSACTION_CARD_ACTIVE]
      ,[HH_TRANSACTION_GROSS_AMT]
      ,[HH_TRANSACTION_OUTGOING_ACH]
      ,[HH_TRANSACTION_OUTGOING_WIRE]
      ,[HH_TRANSACTION_DD_IS_CURRENT]
      ,[HH_TRANSACTION_DD]
      ,[HH_TRANSACTION_PAYROLL_IS_CURRENT]
      ,[HH_TRANSACTION_PAYROLL]
      ,[TRANSACTION_CARD_ACTIVE]
     
      ,[HH_TRANSACTION_COUNT]

      ,[HH_COMMITMENT_DD]
      ,[HH_COMMITMENT_BP]
      ,[HAS_CRA_CMTY_SFR]
      ,[HH_HAS_CRA_CMTY_SFR]
      ,[HH_PREV_CLIENT_DEPOSIT_COMMITMENT]
      ,[HH_DEPOSIT_COMMITMENT]
      ,[HAS_RETURNED_MAIL]
      ,[HH_HAS_RETURNED_MAIL]
      ,[CUST_ACTIVITY_CD]
  
 
      ,[TOTAL_DEPOSITS]
      ,[HAS_DEPOSITS]
      ,[HH_TOTAL_DEPOSITS]
      ,[HH_HAS_DEPOSITS]
  FROM [CustDM].[dbo].[T_CDM_CUST_FEATURES]
'''


cust_dimension_nps = utils.get_data(data.nps_lead_bank_labels) # client info

clientdim = get_data(dimensions)
clientdim.set_index('CDM_CUST_KEY', inplace=True)

clientdim.index.rename('cdm_cust_key', inplace=True)

pd.set_option('display.max_columns', None)
#the 
train =  pd.read_excel('output\monthly_scores_w_labels_payroll_ppc_primary_nps_output_v2.xlsx') #is client recieving payroll

train = train[train['reportingMonth'] == '2020-09-30']

train = train[train['SEGMENT_2'] == 'Personal']

train = train.merge(clientdim,on=['cdm_cust_key'], how='inner')

pd.options.display.max_seq_items = 2000

features = [
          #'cdm_cust_key'

      #  'daysSinceLastTX'
      #  ,'monthlyTXCount'
      #  ,'monthlyMonetaryValue'
      #  ,'monthlyPaymentBillCnt'
       # ,'monthlyRQuartile'
       # ,'monthlyFQuartile'
       # ,'monthlyMQuartile'
      #  'monthlyTotalScore'
        'payrollCnt'
        ,'PPC'
       # ,'threeMonthTXCount'
       # ,'threeMonthAvgMonetaryValue'
       # ,'threeMonthMonetaryValue'
        ,'IS_LEAD_BANK'
      #  ,'PREV_CLIENT_DEPOSIT_COMMITMENT'
      #  , 'DEPOSIT_COMMITMENT'
      #  ,'LOAN_APP_POST_LOAN_LIQUIDITY'
        , 'LOAN_APP_NET_WORTH'
        , 'LOAN_APP_FICO'
        ,'LOAN_APP_DEBT_TO_INCOME'
       # , 'LOAN_APP_INCOME1040'
        ,'LOAN_APP_TOTAL_INCOME'
        ,'LOAN_APP_ASSETS_CASH'
        ,'LOAN_APP_ASSETS_SECURITIES'
        ,'LOAN_APP_ASSETS_RETIREMENT'
        ,'LOAN_APP_ASSETS_TRUST'
        , 'LOAN_APP_ASSETS_OTHER'
       # , 'TRANSACTION_COUNT'
        ,'TRANSACTION_ATM_ACTIVE'
       # ,'TRANSACTION_GROSS_AMT'
       # ,'TRANSACTION_OUTGOING_ACH'
       #,'TRANSACTION_OUTGOING_WIRE'
       #,'TRANSACTION_DD_IS_CURRENT'
       #,'TRANSACTION_DD'
       #,'TRANSACTION_PAYROLL_IS_CURRENT'
       #,'TRANSACTION_PAYROLL'
       ,'IS_HEAD_OF_HOUSEHOLD'
       ,'HH_MEMBERS'
       ,'HH_MEMBERS_WITH_OPEN_PRIMARY_ACCT'
       ,'HH_DEPOSIT_TO_LOAN_RATIO'
       ,'CD_ONLY'
       ,'HAS_CHECKING'
       ,'HAS_SFR'
       ,'HAS_HDR'
       ,'CHECKING'
       ,'ICS'
       #,'DEPOSITS'
       #,'DRAWN'
       #,'COMMITMENT'
       #,'AUM'
       ,'HH_CD_ONLY'
       ,'HH_HAS_CHECKING'
       ,'HH_HAS_SFR'
       ,'HH_HAS_HDR'
       ,'HH_CHECKING'
       ,'HH_ICS'
       #,'HH_DEPOSITS'
       ,'HH_DRAWN'
       ,'HH_COMMITMENT'
       ,'HH_AUM'
       #,'CHECKING_12M'
       #,'HH_CHECKING_12M'
       ,'HAS_ATM_CARD'
       #,'HH_TRANSACTION_ATM_ACTIVE'
       #,'HH_TRANSACTION_CARD_ACTIVE'
       #,'HH_TRANSACTION_GROSS_AMT'
       #,'HH_TRANSACTION_OUTGOING_ACH
       #,'HH_TRANSACTION_OUTGOING_WIRE'
       #,'HH_TRANSACTION_DD_IS_CURRENT'
       #,'HH_TRANSACTION_DD'
       #,'HH_TRANSACTION_PAYROLL_IS_CURRENT'
       #,'HH_TRANSACTION_PAYROLL'
       #,'TRANSACTION_CARD_ACTIVE'
       #,'HH_TRANSACTION_COUNT'
       ,'HAS_CRA_CMTY_SFR'
       ,'HH_HAS_CRA_CMTY_SFR'
       #,'HH_PREV_CLIENT_DEPOSIT_COMMITMENT'
       #,'HH_DEPOSIT_COMMITMENT'
       ,'HAS_RETURNED_MAIL'
       ,'HH_HAS_RETURNED_MAIL'
       ,'CUST_ACTIVITY_CD'
       #,'TOTAL_DEPOSITS'
       #,'HAS_DEPOSITS'
       #,'HH_TOTAL_DEPOSITS'
       #,'HH_HAS_DEPOSITS'
        ]


train_clean = train[features]

train_clean = train_clean.fillna(0)


train_clean.isnull().sum()

train_clean = train_clean.astype(int)

y = train_clean.IS_LEAD_BANK
X = train_clean.drop(columns=['IS_LEAD_BANK'])

train_clean.dtypes

from sklearn.model_selection import train_test_split
X_train,X_test,y_train,y_test = train_test_split(X,y,test_size=0.3,random_state=38)


from sklearn.tree import DecisionTreeClassifier
from sklearn import tree

clf = tree.DecisionTreeClassifier(max_depth=3, criterion="gini")
clf.fit(X_train,y_train)


clf.score(X_test,y_test)


# features_for_graph = [
        
#         #'cdm_cust_key'

#          'daysSinceLastTX'
#         ,'monthlyTXCount'
#         ,'monthlyMonetaryValue'
#         ,'monthlyPaymentBillCnt'
#        # ,'monthlyRQuartile'
#        # ,'monthlyFQuartile'
#        # ,'monthlyMQuartile'
#         ,'monthlyTotalScore'
#         ,'payrollCnt'
#         ,'PPC'
#        # ,'threeMonthTXCount'
#        # ,'threeMonthAvgMonetaryValue'
#        # ,'threeMonthMonetaryValue'
#        # ,'IS_LEAD_BANK'
#         ]


features_for_graph =[
          #'cdm_cust_key'

      #  'daysSinceLastTX'
      #  ,'monthlyTXCount'
      #  ,'monthlyMonetaryValue'
      #  ,'monthlyPaymentBillCnt'
       # ,'monthlyRQuartile'
       # ,'monthlyFQuartile'
       # ,'monthlyMQuartile'
      #  'monthlyTotalScore'
        'payrollCnt'
        ,'PPC'
       # ,'threeMonthTXCount'
       # ,'threeMonthAvgMonetaryValue'
       # ,'threeMonthMonetaryValue'
      #  ,'IS_LEAD_BANK'
      #  ,'PREV_CLIENT_DEPOSIT_COMMITMENT'
      #  , 'DEPOSIT_COMMITMENT'
      #  ,'LOAN_APP_POST_LOAN_LIQUIDITY'
        , 'LOAN_APP_NET_WORTH'
        , 'LOAN_APP_FICO'
        ,'LOAN_APP_DEBT_TO_INCOME'
       # , 'LOAN_APP_INCOME1040'
        ,'LOAN_APP_TOTAL_INCOME'
        ,'LOAN_APP_ASSETS_CASH'
        ,'LOAN_APP_ASSETS_SECURITIES'
        ,'LOAN_APP_ASSETS_RETIREMENT'
        ,'LOAN_APP_ASSETS_TRUST'
        , 'LOAN_APP_ASSETS_OTHER'
       # , 'TRANSACTION_COUNT'
        ,'TRANSACTION_ATM_ACTIVE'
       # ,'TRANSACTION_GROSS_AMT'
       # ,'TRANSACTION_OUTGOING_ACH'
       #,'TRANSACTION_OUTGOING_WIRE'
       #,'TRANSACTION_DD_IS_CURRENT'
       #,'TRANSACTION_DD'
       #,'TRANSACTION_PAYROLL_IS_CURRENT'
       #,'TRANSACTION_PAYROLL'
       ,'IS_HEAD_OF_HOUSEHOLD'
       ,'HH_MEMBERS'
       ,'HH_MEMBERS_WITH_OPEN_PRIMARY_ACCT'
       ,'HH_DEPOSIT_TO_LOAN_RATIO'
       ,'CD_ONLY'
       ,'HAS_CHECKING'
       ,'HAS_SFR'
       ,'HAS_HDR'
       ,'CHECKING'
       ,'ICS'
       #,'DEPOSITS'
       #,'DRAWN'
       #,'COMMITMENT'
       #,'AUM'
       ,'HH_CD_ONLY'
       ,'HH_HAS_CHECKING'
       ,'HH_HAS_SFR'
       ,'HH_HAS_HDR'
       ,'HH_CHECKING'
       ,'HH_ICS'
       #,'HH_DEPOSITS'
       ,'HH_DRAWN'
       ,'HH_COMMITMENT'
       ,'HH_AUM'
       #,'CHECKING_12M'
       #,'HH_CHECKING_12M'
       ,'HAS_ATM_CARD'
       #,'HH_TRANSACTION_ATM_ACTIVE'
       #,'HH_TRANSACTION_CARD_ACTIVE'
       #,'HH_TRANSACTION_GROSS_AMT'
       #,'HH_TRANSACTION_OUTGOING_ACH
       #,'HH_TRANSACTION_OUTGOING_WIRE'
       #,'HH_TRANSACTION_DD_IS_CURRENT'
       #,'HH_TRANSACTION_DD'
       #,'HH_TRANSACTION_PAYROLL_IS_CURRENT'
       #,'HH_TRANSACTION_PAYROLL'
       #,'TRANSACTION_CARD_ACTIVE'
       #,'HH_TRANSACTION_COUNT'
       ,'HAS_CRA_CMTY_SFR'
       ,'HH_HAS_CRA_CMTY_SFR'
       #,'HH_PREV_CLIENT_DEPOSIT_COMMITMENT'
       #,'HH_DEPOSIT_COMMITMENT'
       ,'HAS_RETURNED_MAIL'
       ,'HH_HAS_RETURNED_MAIL'
       ,'CUST_ACTIVITY_CD'
       #,'TOTAL_DEPOSITS'
       #,'HAS_DEPOSITS'
       #,'HH_TOTAL_DEPOSITS'
       #,'HH_HAS_DEPOSITS'
        ]






import graphviz 
import pydotplus
dot_data = tree.export_graphviz(clf, out_file=None,
                               feature_names=features_for_graph,
                               class_names=['Not Lead Bank','Lead Bank'],filled=True,
                                rounded=True,  
                              special_characters=True) 
graph = pydotplus.graph_from_dot_data(dot_data)

#graph = graphviz.Source(dot_data)
graph.write_png('tree-leadbank-notransactions.png')
#graph.render("Gini")
graph

from sklearn import tree
tree.plot_tree(clf)

feat_importance = clf.tree_.compute_feature_importances(normalize=False)

for name, importance in zip(feat_importance, features_for_graph):
    print(name, importance)


'''
did two models, one with a lot of features
another with just 4 features
payment bills count showed up as a top feature in both and the amount of transactions as well. 

'''