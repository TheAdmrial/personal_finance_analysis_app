#%%
import polars as pl
import os
from dotenv import load_dotenv, dotenv_values
import psycopg2
from uploading_data_to_dim_tables import get_connection, get_existing_categories, get_existing_companies
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
'''Most likely I don't need to establish a connection. I just need the uri to make the get and write to the database.'''
# conn = get_connection()
# if conn:
#     print('Connection to the PostgreSQL established successfully.')
# else:
#     print('Connection to the PostgreSQL encountered an error.')
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

#%%
# join in the company

#%%
# rename the new id column to company_id
# drop the company string column

#%% 
# rearrage the column order to 
# 1) date
# 2) amount
# 3) (transaction)type_id (aka category)
# 4) company_id
# 5) description