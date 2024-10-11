#%%
import polars as pl
import os
from dotenv import load_dotenv, dotenv_values
import psycopg2
from uploading_data_to_dim_tables import get_existing_categories, get_existing_companies, get_connection
#%%
# the postgresql db info
# TODO: Where and when to add try-catch statements
load_dotenv('C:/git/personal_fin_app/.env')
test_user = os.environ.get("fin_app_test_db_user")
test_db = os.environ.get("fin_app_test_db_name")
test_pass = os.environ.get("fin_app_test_db_pass")
test_server = os.environ.get("fin_app_test_server_name")
test_port = str(os.environ.get("fin_app_test_port"))
#%%
#-------------PROCESS OF UPLOADING DATA TO THE FACT TABLE-------------#
'''
1) Replace the 'dimention data' with the ids of those tables
2) Load the data, with the replaced 'dimention data', into the database
    - order of the columns should be:
        1) date
        2) amount
        3) transaction_type_id
        4) company_id
        5) description
'''
#%%
##### Getting the category and company data #####
# making the uri for the get functions
uri = "postgresql://%s:%s@%s:%s/%s" % (test_user, test_pass,test_server,test_port, test_db)
#%%
# getting the categories
categories = get_existing_categories(uri = uri)
#%%
# getting the companies
companies = get_existing_companies(uri = uri)
#%%
# Make a function to replace either the category's or company's text with the corresponding id 

# read in the data
semi_clean_data = pl.read_csv('C:/git/personal_fin_app/personal_finance_analysis_app/data/faker_fin_data_clean.csv')
#%%
# joining in the categories
with_categories = semi_clean_data.join(other = categories
                     , left_on = 'category'
                     , right_on = 'type_name' 
                     , how = 'inner')
# %%
# rename the new id column to type_id
# drop the category string column
with_categories2 = with_categories.select(
    pl.col('date')
    , pl.col('amount')
    , pl.col('id').alias('type_id')
    , pl.col('company')
    , pl.col('description')
)

#%%
# join in the company
all_combined = with_categories2.join(other = companies
                                     , left_on = 'company'
                                     , right_on= 'company_name'
                                     , how = 'inner')
#%%
# rename the new id column to company_id
# drop the company string column
all_combined2 = all_combined.select(
     pl.col('date')
    , pl.col('amount')
    , pl.col('type_id')
    , pl.col('id').alias('company_id')
    , pl.col('description')
)
#%% 
# rearrage the column order to 
# 1) date
# 2) amount
# 3) (transaction)type_id (aka category)
# 4) company_id
# 5) description

all_combined2.write_database(table_name = 'transactions'
                             , connection = uri
                             , engine= 'adbc'
                             , if_table_exists='append')