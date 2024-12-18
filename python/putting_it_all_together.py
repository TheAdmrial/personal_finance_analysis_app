
from uploading_data_to_dim_tables import get_existing_categories, get_existing_companies, get_connection

#---------------------ORDER OF OPERATIONS---------------------#
'''
1) Clean the data
    a) selecting out the columns that I need 
    b) getting the names from the description
    c) translating that into a dictionary
    d) ultinately, add the transaction_types and the companies to the data set. 
2) Add new data to the dim tables
    This is done first because if I were to add the data to the fact table there would be no ids for the potentially new companies and transaction_types
3) Add new data to the fact table
    Keep in mind, I need to replace the 'dimention columns' with their corresponding ids.
4) connect the PowerBI dashboard with the Database
'''