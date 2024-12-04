#%%
import polars as pl
import os
from dotenv import load_dotenv, dotenv_values
import psycopg2
from typing import Optional, Union, Set, Dict

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
# making a function to connect to the postgresql DB
def get_connection(
        db: Optional[str] = None
        , user: Optional[str] = None
        , password: Optional[str] = None
        , host: Optional[str] = None
        , port: Optional[str] = None
) -> Optional[psycopg2.extensions.connection]:
    '''
    Establish a connection to the PostgreSQL database with comprehensive error handling.
    
    Args:
        db (str, optional): Database name. Uses environment variable if not provided.
        user (str, optional): Database username. Uses environment variable if not provided.
        password (str, optional): Database password. Uses environment variable if not provided.
        host (str, optional): Database host. Uses environment variable if not provided.
        port (str, optional): Database port. Uses environment variable if not provided.
    
    Returns:
        psycopg2.extensions.connection or None: Database connection object or None if connection fails
    '''
    try:
        # Use environment variables as fallback if parameters not provided
        database = db or os.environ.get('fin_app_test_db_name')
        username = user or os.environ.get('fin_app_test_db_user')
        passwd = password or os.environ.get('fin_app_test_db_pass')
        server = host or os.environ.get('fin_app_test_server_name')
        db_port = port or str(os.environ.get('fin_app_test_port'))

        # Validate all required connection parameters
        if not all([database, username, passwd, server, db_port]):
            raise ValueError("Missing reqired database connection parameters")
        
        connection = psycopg2.connect(
            database = database
            , user = username
            , password = passwd
            , host = server 
            , port = db_port 
        )

        # Additional connection configuration
        connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        print('Connection to PostgreSQL established successfully.')
        return connection
    
    except psycopg2.Error as db_error:
        print(f"Database Connection Error: {db_error}")
        return None
    except ValueError as val_error:
        print(f"Configuration Error: {val_error}")
        return None
    except Exception as e:
        print(f"Unexpected error connecting to database: {e}")
        return None
    
#%% connecting to the database
conn = get_connection()
if conn:
    print('Connection to the PostgreSQL established successfully.')
else:
    print('Connection to the PostgreSQL encountered an error.')
#%%
# uri = "postgresql://username:password@server:port/database"

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
# **********get_existing_categories**********
# step 1 query the transaction_type and company tables
# transaction_query = 'SELECT * FROM transaction_type ORDER BY id'
# transaction_results = pl.read_database_uri(query=transaction_query, uri=uri)

# step 2 
# **********get_existing_compnaies**********
# company_query = 'SELECT * FROM company ORDER BY id'
# company_results = pl.read_database_uri(query=company_query, uri=uri)

#%%
def get_existing_categories(uri: str) -> Optional[pl.DataFrame]:
    '''
    Retrieve existing transaction categories from the database with error handling.
    
    Args:
        uri (str): Database connection URI
    
    Returns:
        Optional[pl.DataFrame]: DataFrame of existing categories or None if error occurs
    '''
    try:
        categories = pl.read_database_uri(
            query='SELECT * FROM transaction_type ORDER BY transaction_type_id'
            , uri=uri
            )
        return categories
    
    except Exception as e:
        print(f'Error retrieving categories: {e}')
        return None

def get_existing_companies(uri = str):
    '''
    Retrieve existing transaction companies from the database with error handling.
    
    Args:
        uri (str): Database connection URI
    
    Returns:
        Optional[pl.DataFrame]: DataFrame of existing categories or None if error occurs
    '''
    try:
        companies = pl.read_database_uri(
            query='SELECT * FROM company ORDER BY company_id'
            , uri=uri)
        return companies
    
    except Exception as e:
        print(f'Error retrieving categories: {e}')
        return None

#%%
# **********existing_options_to_list**********
# step 3 compare the dictionary values with what exisits in the list from the db. Kick out any values that match from the dictionary. 

# getting the length of the results because the column will grow
# max_cat_id = (len(transaction_results.select(pl.col('type_name'))))
# saving the list of existing categories
# categories = list(transaction_results[0:max_cat_id,1])

#%%
def existing_options_to_list(results = pl.DataFrame, name_column = ('type','company')):
    '''
    This function will get all of the existing items from the database to a list. 
    This is assuming that you are using the pl.read_database_uri to get your results
    (See either get_existing_categories or get_existing_compnaies)
    
    '''
    #TODO: grab the column names from the DataFrame and put the proper column name in the pl.col() call to get max_id
    #^^ this needs to be done! DON'T HARD CODE COLUMN NAMES!
    if name_column == 'type':
        max_id = (len(results.select(pl.col('type_name'))))
        return list(results[0:max_id, 1])
    elif name_column == 'company':
        max_id = (len(results.select(pl.col('company_name'))))
        return list(results[0:max_id, 1])
    else:
        print('You have not selected a proper column name. Please choose only type or company')
#%%
# **********get_items_to_add**********
#function 1 Cleaning out the dictionary with existing values pt1
# comparing dict values to the list values
# categories_new = transaction_type
# cats_to_pop = []
# print(categories_new)
#%%
#function 1 Cleaning out the dictionary with existing values pt2
# for key, value in transaction_type.items():
#     for i in range(len(categories)):
#         if value in categories[i]:
#             cats_to_pop.append(key) 
#%%
#function 1 Cleaning out the dictionary with existing values pt3
# return the new dict with the popped values
# for k in cats_to_pop:
#     categories_new.pop(k)
#%%
# print(transaction_type)
#%%
#function 2 getting the unique categories
# categories_new_unique = set()
# for value in categories_new.values():
#     categories_new_unique.add(value)
#%%
#TODO: Add a function to make a dictionary with the existing vaules. (Likely there is a df_to_dict function already)
def get_items_to_add(items_from_user = dict, results = list):
    '''
    This function will make a unique list of items to add to the database. 
    '''
    all_items = results
    new_items = items_from_user
    items_to_remove = []
    for key, value in items_from_user.items():
        for i in range(len(all_items)):
            if value in all_items[i]:
                items_to_remove.append(key) 
    # Subtracting out the items that I don't need. (Because they already exist in the database)
    for k in items_to_remove:
        new_items.pop(k)
    # making a unique list
    categories_new_unique = set()
    for value2 in new_items.values():
        categories_new_unique.add(value2)
    return categories_new_unique


#%%
# **********adding_new_cat_data***********
# Create a cursor using the connection object
# curr = conn.cursor()
#%% # run your SQL query
# not sure if curr.execute likes being in a for loop. Need to investigate how to properly add data.
# for h in categories_new_unique:
#     insert_stmt = '''INSERT INTO transaction_type (id, type_name) VALUES (DEFAULT, %s);'''
#     curr.execute(insert_stmt, (h,))
    #conn.rollback
#%%
# with every commit need to pair with a close
# once you're done with a connection you need to close it. 
# conn.commit()
# conn.close()

#%%
def adding_new_cat_data(conn: psycopg2.extensions.connection
                        , unique_items_to_add: Set[str]) -> bool:
    '''
    Add new transaction categories to the database with robust error handling.
    
    Args:
        conn (psycopg2.extensions.connection): Active database connection
        unique_items_to_add (Set[str]): Set of unique categories to add
    
    Returns:
        bool: True if successful, False otherwise
    '''
    if not conn or conn.closed:
        print('Invalid database connection.')
        return False
    
    try:
        with conn.cursor() as curr:
            for category in unique_items_to_add:
                # Validate input to prevent SQL injection
                if not isinstance(category, str) or not category.strip():
                    print(f'Skipping invalid category: {category}')
                    continue
                insert_stmt = '''INSERT INTO transaction_type (transaction_type_id, type_name) VALUES (DEFAULT, %s);'''
                curr.execute(insert_stmt, (h,))
        conn.commit()
        print(f'Successfully added {len(unique_items_to_add)} new transactions types.')
        return True

    except psycopg2.Error as db_error:
        print(f'Database Error: {db_error}')
        conn.rollback()
        return False
    except Exception as e:
        print(f'Unexpected error adding transaction types: {e}')
        conn.rollback()
        return False
    finally:
        #Best practice to close connect when done
        if conn and not conn.closed:
            conn.close()

            
#%%
def adding_new_co_data(conn, unique_items_to_add = set):
    '''

    This function will attempt to add the new companies to its dimention table. 
    conn - This needs to be the database connection. This needs to be established beforehand
    unique_items_to_add - this is the set that will have the unique items to add.
    '''
    curr = conn.cursor()
    for h in unique_items_to_add:
        insert_stmt = '''INSERT INTO company (company_id, company_name) VALUES (DEFAULT, %s);'''
        curr.execute(insert_stmt, (h,))
    conn.commit()
    conn.close()

    return print('Done! New values added to the company table.')


#%%
# TODO:adding data to the transaction table
# TODO:test the whole workflow
# %%
#%%
# list out the workflow
'''
List of functions in order:
1) get_existing_categories & get_existing_companies
2) existing_options_to_list
3) get_items_to_add
4) get_connection
5) adding_new_cat_data & adding_new_co_data
'''
#%%
#---------TESTING THE FUNCTIONS---------------#
uri = "postgresql://%s:%s@%s:%s/%s" % (test_user, test_pass,test_server,test_port, test_db)
# Step 1: get the existing companies (or categories)
co_results = get_existing_companies(uri)
type_results = get_existing_categories(uri)
#%%
# Step 2: getting the existing options into a list
list_existing_cos = existing_options_to_list(co_results,'company')
list_existing_types = existing_options_to_list(type_results,'type')
#%%
# Step 3: getting the items to add to the database
cos_to_add = get_items_to_add(company, list_existing_cos)
types_to_add = get_items_to_add(transaction_type, list_existing_types)
#%%
# Step 4: 
# a) Connecting to the database
conn = get_connection()
# adding_new_co_data(conn = conn, unique_items_to_add=cos_to_add)
#%%
# b) Writing the new data to the database
# conn = get_connection()
adding_new_cat_data(conn = conn, unique_items_to_add=types_to_add)
# %%
