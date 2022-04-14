
from helpers import utils
from data import data

import pandas as pd
import numpy as np
import scipy.stats as stats
import seaborn as sns
import matplotlib.pyplot as plt
import pprint as pp

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



#data
data = '''


    SELECT * FROM   [DataScienceAndAnalytics].dev.[lead_bank_binary_hh_feats_v2]
where NPS2020_respondent = 1
/*
SELECT * 
FROM [DATASCIENCEANDANALYTICS].[CLIENT_FEEDBACK].[SURVEY_DATA_TABLEAU] nps
LEFT JOIN [DATASCIENCEANDANALYTICS].[STAGE].[03_ACCT_TXNS_SUMMARY_NPS2020_HH_LEVEL] TXNS_HH_LEVEL
ON (TXNS_HH_LEVEL.PARENT_CDM_CUST_KEY_20200930= NPS.parent_cdm_cust_key_0930)
LEFT JOIN [DATASCIENCEANDANALYTICS].[STAGE].[HH_LEVEL_PRODUCT_FEATURES_09302020_V2] HH_PROD_F
ON (HH_PROD_F.PARENT_CDM_CUST_KEY = NPS.parent_cdm_cust_key_0930)
LEFT JOIN (

SELECT 
 parent_cdm_cust_key
,max(has_acct_vested_as_payroll) has_acct_vested_as_payroll
,max(has_acct_vested_as_operational) has_acct_vested_as_operational
,max(has_acct_vested_as_transactional) has_acct_vested_as_transactional
,max(uses_mobile_app) uses_mobile_app
,max(has_visited_branch) has_visited_branch
,max(txn_performed_in_branch) txn_performed_in_branch
,max(hh_uses_ACI_ACImodel) hh_uses_ACI_ACImodel


FROM [DATASCIENCEANDANALYTICS].[STAGE].[01_CLIENT_ACCT&TXN_FEATURES_09302020] AS CL_FEATS

	GROUP BY PARENT_CDM_CUST_KEY



)  CL_FEATURES
ON (CL_FEATURES.PARENT_CDM_CUST_KEY = NPS.parent_cdm_cust_key_0930 )

WHERE 1=1
	AND VALID_LEAD_BANK = 1
	AND SURVEY_YEAR = '2020-09-30'
*/


'''






clientdim = get_data(data)
clientdim.set_index('parent_cdm_cust_key', inplace=True)

clientdim.index.rename('parent_cdm_cust_key', inplace=True)

pd.set_option('display.max_columns', None)
#the 
train = clientdim  #is client recieving payroll

train = train[train['hh_acct_types'] != 'Consumer only']




features = [

'hh_has_deposit'
,'hh_has_deposit_no_FX'
,'hh_has_loan'
,'hh_has_pwm'
,'hh_has_slr'
,'hh_has_plp'
,'hh_has_ploc'
,'hh_has_trust'
,'hh_has_frsc'
,'hh_has_frim'
,'hh_has_eagle_invest'
,'hh_has_chk'
,'hh_has_cre'
,'hh_has_loan_heloc_comm'
,'hh_has_loan_heloc_cons'
,'hh_has_loan_constr_comm'
,'hh_has_loan_constr_cons'
,'hh_has_loan_mf'
,'hh_has_loan_sfr'
,'hh_has_loan_capital_call'
,'hh_has_loan_other_comm'
,'hh_has_loan_other_cons'
,'hh_has_loan_other'
,'hh_had_closed_loan'
,'hh_has_active_checking'
,'hh_has_auto_debit'
,'hh_q2_active'
,'hh_uses_ACI'
,'hh_has_mobile_check_L3M'
,'hh_uses_mobile_app'
,'hh_has_mmmf'
,'hh_has_dir_dep'
,'hh_9_txns_L3M'
,'hh_txn_performed_in_branch'
,'hh_has_incoming_wires_L3M'
,'hh_has_incoming_wires_L6M'
,'hh_has_outgoing_wires_L3M'
,'hh_has_outgoing_wires_L6M'
,'hh_has_acct_vested_as_payroll'
,'hh_has_acct_vested_as_operational'
,'hh_has_acct_vested_as_transactional'
,'hh_score_higher_lev'
,'hh_has_bill_pay_L1M'
,'hh_has_atm_txn_L1M'
,'hh_has_eagle'
,'hh_has_wire_L3M'
,'hh_txn_performed_in_branch_higher_conf'
,'hh_has_sv_cd'

,'is_lead_bank'
]




#PICK SPECIFIC FEATURES HERE
#peak features pp.pprint(train.columns.tolist())

cols_T_F = []
for col in features:
    if len(set(['N', 'Y']) & set(train[col].unique())) > 0:
        cols_T_F.append(col)
        #columns_of_X.pop(columns_of_X.index(col))
print(cols_T_F)

#Need to turn Y/N into 1/0 values
for col in cols_T_F:
    #print(col)
    train[col] = train[col].apply(lambda x: 1 if x =='Y' else 0)





train_clean = train[features]

train_clean = train_clean.fillna(0)


#train_clean.isnull().sum()

train_clean = train_clean.astype(int)

y = train_clean.is_lead_bank
X = train_clean.drop(columns=['is_lead_bank'])

train_clean.dtypes

from sklearn.model_selection import train_test_split
X_train,X_test,y_train,y_test = train_test_split(X,y,test_size=0.3,random_state=38)


from sklearn.tree import DecisionTreeClassifier
from sklearn import tree

clf = tree.DecisionTreeClassifier(max_depth=3, criterion="gini")
clf.fit(X_train,y_train)


clf.score(X_test,y_test)




graphfeatures = [
'hh_has_deposit'
,'hh_has_deposit_no_FX'
,'hh_has_loan'
,'hh_has_pwm'
,'hh_has_slr'
,'hh_has_plp'
,'hh_has_ploc'
,'hh_has_trust'
,'hh_has_frsc'
,'hh_has_frim'
,'hh_has_eagle_invest'
,'hh_has_chk'
,'hh_has_cre'
,'hh_has_loan_heloc_comm'
,'hh_has_loan_heloc_cons'
,'hh_has_loan_constr_comm'
,'hh_has_loan_constr_cons'
,'hh_has_loan_mf'
,'hh_has_loan_sfr'
,'hh_has_loan_capital_call'
,'hh_has_loan_other_comm'
,'hh_has_loan_other_cons'
,'hh_has_loan_other'
,'hh_had_closed_loan'
,'hh_has_active_checking'
,'hh_has_auto_debit'
,'hh_q2_active'
,'hh_uses_ACI'
,'hh_has_mobile_check_L3M'
,'hh_uses_mobile_app'
,'hh_has_mmmf'
,'hh_has_dir_dep'
,'hh_9_txns_L3M'
,'hh_txn_performed_in_branch'
,'hh_has_incoming_wires_L3M'
,'hh_has_incoming_wires_L6M'
,'hh_has_outgoing_wires_L3M'
,'hh_has_outgoing_wires_L6M'
,'hh_has_acct_vested_as_payroll'
,'hh_has_acct_vested_as_operational'
,'hh_has_acct_vested_as_transactional'
,'hh_score_higher_lev'
,'hh_has_bill_pay_L1M'
,'hh_has_atm_txn_L1M'
,'hh_has_eagle'
,'hh_has_wire_L3M'
,'hh_txn_performed_in_branch_higher_conf'
,'hh_has_sv_cd'
 
        ]


import graphviz 
import pydotplus
dot_data = tree.export_graphviz(clf, out_file=None,
                               feature_names=graphfeatures,
                               class_names=['Not Lead Bank','Lead Bank'],filled=True,
                                rounded=True,  
                              special_characters=True) 
graph = pydotplus.graph_from_dot_data(dot_data)

#graph = graphviz.Source(dot_data)
graph.write_png('tree-leadbank-new.png')
#graph.render("Gini")
graph

from sklearn import tree
tree.plot_tree(clf)

feat_importance = clf.tree_.compute_feature_importances(normalize=True)

for name, importance in zip(feat_importance, graphfeatures):
    print(name, importance)


