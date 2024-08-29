#%%
import polars as pl
import os
from dotenv import load_dotenv, dotenv_values
#%%
# the postgresql db info
load_dotenv('C:/git/personal_fin_app/.env')
test_user = os.environ.get("fin_app_test_db_user")
test_db = os.environ.get("fin_app_test_db_name")
test_pass = os.environ.get("fin_app_test_db_pass")
test_server = os.environ.get("fin_app_test_server_name")
test_port = str(os.environ.get("fin_app_test_port"))
#%%
# uri = "postgresql://username:password@server:port/database"
uri = "postgresql://%s:%s@%s:%s/%s" % (test_user, test_pass,test_server,test_port, test_db)
# print(uri)
# the dataframe to load into the db
#%%
'''
step 1: query the transaction_type and company tables for all the values
step 2: make a list/dictionary with the existing values 
    Step 2a: grab the list of values from the column
step 3: compare the current table contents with the new data
step 4: if there are values that aren't in dict (db tables) then add the new company or transaction_type
step 5: add columns to the df that convert the company and transaction_type to a number. Drop the string company and transaction_type columns
step 6: load the df into the main transaction table 
'''
#%%
company = { 'Football Is Good':'Ftbl Co'
           , 'Maple Gaming':'Maple Games'
           , 'Andy shoes':"Andy's"
           , 'Apple Natural':'Nat Apple'
           , 'compression wear':'Boston'
           , 'goalkeeper gloves':'Carbonite'
           , 'layout consists':'The Auto'
           , 'bonded black leather':'Ergo Chair'
           , 'sport bikes':'Nagasaki Lander'
           , 'B340':'Apollotech'
           , '13 9370':'ABC Co'
           , 'formal shirts':'New Fits'}
transaction_type = {'Apollotech': 'Income'
                    , 'New ABC': 'Income'
                    , 'Maple Gaming': 'Groceries'
                    , 'shoes are designed': 'Groceries'
                    , 'beautiful range': 'Fun Money'
                    , 'stomthieng': 'stupid'
                    , 'Football Is Good': 'Utilities'
                    , 'advanced compression': 'Utilities'
                    , 'goalkeeper gloves': 'Groceries'
                    , 'layout consists': 'Car Payment'
                    , 'automobile layout': 'Car Payment'
                    , 'bonded black leather': 'Rent'
                    , 'name of several': 'Groceries'
                    , 'fits and styling': 'Clothes'}
#%%
# step 1 query the transaction_type and company tables
transaction_query = 'SELECT * FROM transaction_type'
company_query = 'SELECT * FROM company'

# step 2 
transaction_results = pl.read_database_uri(query=transaction_query, uri=uri)
company_results = pl.read_database_uri(query=company_query, uri=uri)

#%%
# step 3 compare the dictionary values with what exisits in the list from the db. Kick out any values that match from the dictionary. 


#%%
# step 4 add the dictionary values that still exist to either to the company table or the transaction_type table


# actually writing to the db
# df.write_database(table_name="records", connection=uri, engine="adbc")
# %%
