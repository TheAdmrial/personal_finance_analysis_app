#%%
import polars as pl
import os
from dotenv import load_dotenv, dotenv_values
import psycopg2
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
def get_connection():
    '''
    This function connects to the database with the credentials provided above. 
    '''
    try:
        return psycopg2.connect(
            database = test_db
            , user = test_user
            , password = test_pass
            , host = test_server
            , port = test_port
        )
    except:
        return False

#%%
#-------------PROCESS OF UPLOADING DATA TO THE FACT TABLE-------------#
'''
1) Replace the 'dimention data' with the ids of those tables
2) Load the data, with the replaced 'dimention data', into the database
'''