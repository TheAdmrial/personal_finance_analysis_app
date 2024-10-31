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
# making a function to connect to the postgresql DB
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
def get_existing_categories(uri = str):
    '''
    The goal of this function is to get all the current transaction_types that are in the database. This will be used to compare againsts the potentially new transaction_types. 
    '''
    return pl.read_database_uri(query='SELECT * FROM transaction_type ORDER BY transaction_type_id', uri=uri)
def get_existing_companies(uri = str):
    '''
    The goal of this function is to get all the current companies that are in the database. This will be used to compare againsts the potentially new companies. 
    '''
    return pl.read_database_uri(query='SELECT * FROM company ORDER BY company_id', uri=uri)
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
def adding_new_cat_data(conn, unique_items_to_add = set):
    '''

    This function will attempt to add the new categories to its dimention table. 
    conn - This needs to be the database connection. This needs to be established beforehand
    unique_items_to_add - this is the set that will have the unique items to add.
    '''
    curr = conn.cursor()
    for h in unique_items_to_add:
        insert_stmt = '''INSERT INTO transaction_type (transaction_type_id, type_name) VALUES (DEFAULT, %s);'''
        curr.execute(insert_stmt, (h,))
    conn.commit()
    conn.close()

    return print('Done! New values added to the transaction_type table.')


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
# co_results = get_existing_companies(uri)
type_results = get_existing_categories(uri)
#%%
# Step 2: getting the existing options into a list
# list_existing_cos = existing_options_to_list(co_results,'company')
list_existing_types = existing_options_to_list(type_results,'type')
#%%
# Step 3: getting the items to add to the database
# cos_to_add = get_items_to_add(company, list_existing_cos)
types_to_add = get_items_to_add(transaction_type, list_existing_types)
#%%
# Step 4: 
# a) Connecting to the database
conn = get_connection()
#%%
# b) Writing the new data to the database
# adding_new_co_data(conn = conn, unique_items_to_add=cos_to_add)
adding_new_cat_data(conn = conn, unique_items_to_add=types_to_add)
# %%
