#%%
import polars as pl
from polars import col as c
import plotnine as plot

#%%
# Reading in the data
fin_dat = pl.read_csv('c:/git/personal_fin_app/personal_finance_analysis_app/data/faker_fin_data.csv'
                      ,truncate_ragged_lines=True)

# TODO: make an is_income column
#%%
# Cleaning up the Date column: cutting off this part from the string -> .439Z

fin_dat_clean = fin_dat.with_columns(
    pl.col("Date").str.to_date('%Y-%m-%dT%H:%M:%S', strict=False).alias('new_date')
)

#%%
# Transforming the Date column from str to datetime
fin_dat_clean = fin_dat_clean.select(
    pl.col("new_date").alias('date')
    ,pl.col("Amount").alias('amount')
    ,pl.col("Description").alias('description')
)
#%%
# building the expense type dicitonary 
transaction_type = {#{'What I'm looking to match on':'What I want in the column'}
    'Apollotech':'Income'
    , 'New ABC':'Income'
    }

#%%
#building the adding company function
# FROM: ChatGPT 8/13/2024
# def map_dict_to_column(df, string_col, new_col, mapping_dict):
#     # Initialize the new column with None
#     df = df.with_columns(pl.lit(None).alias(new_col))
    
#     # Loop through the dictionary and create a mask for each value
#     for key, value in mapping_dict.items():
#         df = df.with_columns(
#             pl.when(pl.col(string_col).str.contains(value, literal=True))
#               .then(pl.lit(key))
#               .otherwise(pl.col(new_col))
#               .alias(new_col)
#         )
    
#     return df
#%% 
# my recreation of the chatgpt function
def map_dict_to_col(df = pl.DataFrame, string_col = str, new_col = str, mapping_dict = dict):
    '''
    This fn will take a dictionary where the Key is the searching criteria for the string_col (ideally a description column) and add a new column to the data frame you're working with. 
    As an example, pass in a dictionary of {'company names to find':'full name of company to show in the column'}

    df = dataframe that you're working with
    string_col = the name of a description-like column you're looking for
    new_col = the name of the new column to add
    mapping_dict = the dictionary of the values to apply to you're working dataframe. 
    '''
    #make an empty column with None
    df = df.with_columns(pl.lit(None).alias(new_col))

    #loop through the dictionary and create a mask for each value
    for key, value in mapping_dict.items():
        df = df.with_columns(
            pl.when(pl.col(string_col).str.contains(key, literal = True))
            .then(pl.lit(value))
            .otherwise(pl.col(new_col))
            .alias(new_col)
        )
    return df
#%%
def filling_in_new_values(df = pl.DataFrame, desc_col_name = str, column_name = str, dict_to_update = dict):
    '''
    The idea behind this fn is that every month you'll be adding your bank statment.
    There will most likely be newer transactions that need to be categoriezed. 
    This fn will ask how you'd like to do so.

    df = dataframe that you're working with
    desc_col_name = the name of a description-like column you're looking for
    column_name = the name of the column with possible null values
    dict_to_update = the dictionary of the values to apply to you're working dataframe. 
    '''
    df_null_values = df.select(column_name).null_count()[0,0]
    while df_null_values != 0:
        print('Number of Null Values to fill: %s' % df_null_values)
        print('\n')
        top_desc = df.select(c(desc_col_name, column_name)).filter(c(column_name).is_null()).head(1)
        print(top_desc[0,0])
        print()
        print('What would you like to match on?')
        match_on = input("What would you like to match:")
        print()
        print('What name do you want in the column?')
        name_in_col = input("What name will go in the column:")
        print()
        print('Adding %s and %s to the dictionary' % (match_on, name_in_col))
        dict_to_update.update({match_on:name_in_col})
        df = map_dict_to_col(df, desc_col_name, column_name, dict_to_update)
        df_null_values = df.select(c(column_name)).null_count()[0,0]
    
    return df

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
# Apply the function to company
df = map_dict_to_col(fin_dat_clean, "description", "company", company)
df = map_dict_to_col(df, "description", "category", transaction_type)
print(df)
#%%
# filling in the other values that in the company column
df = filling_in_new_values(df, "description", "company", company)
df = filling_in_new_values(df, "description", "category", transaction_type)
#%%
print(df)
print(company)
print(transaction_type)
#%%
# write the data to a csv file in the data folder
df.write_csv(file = 'C:/git/personal_fin_app/personal_finance_analysis_app/data/faker_fin_data_clean.csv')