#%%
import polars as pl
from polars import col as c
import plotnine as plot
from typing import Dict, Optional

#%%
# Reading in the data
fin_dat = pl.read_csv('c:/git/personal_fin_app/personal_finance_analysis_app/data/faker_fin_data.csv'
                      ,truncate_ragged_lines=True)

# TODO: programatically make a dictionary of the company/category and search terms
# TODO: Add a provision to category that searches based on dollar amount and date 
# For example, if you pay rent or utilities through venmo, then the only way to specify what category that expense belongs to is the date and amount. Or a situation where you pay some utilites and all rent to the same account, but on different transactions, then the only way to differentiate those expenses is on dollar amount, and possibly date if they're paid on different days. 
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
def map_dict_to_col(df: pl.DataFrame
                    , string_col: str
                    , new_col: str
                    , mapping_dict: dict) -> pl.DataFrame:
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
def filling_in_new_values(df: pl.DataFrame
                          , desc_col_name: str
                          , column_name: str
                          , dict_to_update: dict
                          , amount_context: bool = True
                          ) -> tuple[pl.DataFrame, Dict[str,str]] :
    '''
    The idea behind this fn is that every month you'll be adding your bank statment.
    There will most likely be newer transactions that need to be categoriezed. 
    This fn will ask how you'd like to do so.

    df = dataframe that you're working with
    desc_col_name = the name of a description-like column you're looking for
    column_name = the name of the column with possible null values
    dict_to_update = the dictionary of the values to apply to you're working dataframe. 

    Args:
        df (pl.DataFrame): DataFrame to process
        desc_col_name (str): Column name for description
        column_name (str): Column to fill with new values
        dict_to_update (Dict[str, str]): Dictionary to update with new mappings
        amount_context (bool): Whether to include amount context in manual entry
    
    Returns:
        tuple: Updated DataFrame and updated dictionary
    '''
    def get_transaction_context(row):
        """Provide context for the transaction to aid categorization."""
        context = f"Description: {row[desc_col_name]}"
        if amount_context:
            context += f"\nAmount: ${row['amount']:.2f}"
        return context


    df_null_values = df.select(column_name).null_count()[0,0]

    while df_null_values != 0:
        print(f'\n--- Categorization Assistant ---')
        print(f'Number of Uncategorized Transactions: {df_null_values}')
        
        # Get the first uncategorized transaction

        top_transaction = df.select(
            pl.col(desc_col_name)
            ,pl.col('amount')
            ,pl.col(column_name)
        ).filter(c(column_name).is_null()).head(1)

        # Extract transaction details
        trans_desc = top_transaction[0, desc_col_name]
        trans_amount = top_transaction[0, 'amount']

        # Provide transaction context
        print("\n--- Transaction Context ---")
        print(f"Description: {trans_desc}")
        print(f"Amount: ${trans_amount:.2f}")

        # Enhanced input prompts
        print("\nHow would you like to categorize this transaction?")
        match_options = [
            "1. Add Company",
            "2. Add Category",
            "3. Skip for Now",
            "4. View Recent Similar Transactions"
        ]

        for option in match_options:
            print(option)
        
        choice = input("Enter your choice (1-4): ")
        
        if choice == '1':
            company_name = input("Enter the company name: ")
            dict_to_update[trans_desc] = company_name
        elif choice == '2':
            category_name = input("Enter the transaction category: ")
            dict_to_update[trans_desc] = category_name
        elif choice == '3':
            # Simple skip
            break
        elif choice == '4':
            # Find and display similar recent transactions
            similar_transactions = df.filter(
                (pl.col(desc_col_name).str.contains(trans_desc, literal=True)) | 
                (pl.col('amount') == trans_amount)
            ).select(desc_col_name, 'amount', column_name)
            
            print("\n--- Similar Recent Transactions ---")
            print(similar_transactions)
            
            continue
        else:
            print("Invalid choice. Please try again.")
            continue
        
        # Re-run categorization with updated dictionary
        df = map_dict_to_col(df, desc_col_name, column_name, dict_to_update)
        df_null_values = df.select(column_name).null_count()[0, 0]
    
    return df, dict_to_update

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