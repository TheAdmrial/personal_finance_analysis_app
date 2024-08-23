import polars as pl

# the postgresql db info
'''
TODO: make a test db to upload the test data into
TODO: make a .env file with the username and password for the database
TODO: reference said .env file 
'''
uri = "postgresql://username:password@server:port/database"
# the dataframe to load into the db
'''
step 1: query the transaction_type and company tables for all the values
step 2: make a dictionary with the existing values 
step 3: compare the current table contents with the new data
step 4: if there are values that aren't in dict (db tables) then add the new company or transaction_type
step 5: add columns to the df that convert the company and transaction_type to a number. Drop the string company and transaction_type columns
step 6: load the df into the main transaction table 
'''

df = pl.DataFrame({"foo": [1, 2, 3]})





# actually writing to the db
df.write_database(table_name="records", connection=uri, engine="adbc")