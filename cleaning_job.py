import json
import pandas as pd
import uuid
from Pre_cleaning_job import pre_cleaning

with open('all_data_22.json', 'r', encoding='utf-8') as f:
    all_data_22 = json.load(f)

def cleaning_job_1(all_data_22):
    # Call the function and grab its output
    cleaned = pre_cleaning(all_data_22)

    # Extract each list by key
    extracted_property_history = cleaned["extracted_property_history"]
    extracted_property_agent   = cleaned["extracted_property_agent"]
    extracted_property_officer = cleaned["extracted_property_officer"]
    extracted_property_sales   = cleaned["extracted_property_sales_info"]
    extracted_rental_history   = cleaned["extracted_rental_history"]
    extracted_rental_listing_info = cleaned["extracted_rental_listing_info"]

    #### Creating the dataframes for sales_listings data points
    property_history_df = pd.DataFrame(extracted_property_history)
    property_agent_df = pd.DataFrame(extracted_property_agent)
    property_officer_df = pd.DataFrame(extracted_property_officer)
    property_sales_info_df = pd.DataFrame(extracted_property_sales)

    # ***Reordering property_history_df***
    property_history_df['sales_history_id'] = None
    for num in property_history_df.index:
        property_history_df.at[num, 'sales_history_id'] = str(uuid.uuid4())
    
    # Move history_id to be the very first column
    property_history_df.insert(0, 'sales_history_id', property_history_df.pop('sales_history_id'))
    
    # Pop out the old “id” column and insert it as “sales_id” in position 1
    property_history_df.insert(1, 'sales_id', property_history_df.pop('id'))

    # ***Reordering property_agent_df***
    property_agent_df['agent_id'] = None
    for num in property_agent_df.index:
        property_agent_df.at[num, 'agent_id'] = str(uuid.uuid4())
    property_agent_df.insert(0, 'agent_id', property_agent_df.pop('agent_id'))
    property_agent_df.insert(1, 'sales_id', property_agent_df.pop('id'))

    # ***Reordering property_officer_df***
    property_officer_df['officer_id'] = None
    for num in property_officer_df.index:
        property_officer_df.at[num, 'officer_id'] = str(uuid.uuid4())
    property_officer_df.insert(0, 'officer_id', property_officer_df.pop('officer_id'))
    property_officer_df.insert(1, 'sales_id', property_officer_df.pop('id'))

    # *****Reordering property_sales_info_df*****
    property_sales_info_df.insert(0, 'sales_id', property_sales_info_df.pop('id'))

    #### Creating the dataframes for rental_listings data points
    rental_history_df = pd.DataFrame(extracted_rental_history)
    rental_listing_info_df = pd.DataFrame(extracted_rental_listing_info)

    # ***Reordering rental_history_df***
    rental_history_df['rental_history_id'] = None
    for num in rental_history_df.index:
        rental_history_df.at[num, 'rental_history_id'] = str(uuid.uuid4())
    rental_history_df.insert(0, 'rental_history_id', rental_history_df.pop('rental_history_id'))
    rental_history_df.insert(1, 'rental_id', rental_history_df.pop('id'))

    # *****Reordering rental_listing_info_df*****
    rental_listing_info_df.insert(0, 'rental_id', rental_listing_info_df.pop('id'))

    return {
        'property_history_df': property_history_df,
        'property_agent_df': property_agent_df,
        'property_officer_df': property_officer_df,
        'property_sales_info_df': property_sales_info_df,
        'rental_history_df': rental_history_df,
        'rental_listing_info_df': rental_listing_info_df
    }


# x = cleaning_job_1(all_data_22)
# print(x)


def cleaning_job_2(all_data):
    # 1. Call the function and grab its output
    cleaned_2 = cleaning_job_1(all_data)

    # 2. Extract each list by key
    property_history_df = cleaned_2["property_history_df"]
    property_agent_df = cleaned_2["property_agent_df"]
    property_officer_df = cleaned_2["property_officer_df"]
    property_sales_info_df = cleaned_2["property_sales_info_df"]
    rental_history_df = cleaned_2["rental_history_df"]
    rental_listing_info_df = cleaned_2["rental_listing_info_df"]

    # Cleaning procedure for property_history_df
    # Drop multiple columns, returning a new DataFrame
    property_history_df.drop(columns=["event", "listingType", "removedDate"], inplace=True)
    # Dealing withd duplicates
    property_history_df.drop_duplicates(subset=['sales_id'], keep='first')
    # Dealing with datetime
    property_history_df['listedDate']  = pd.to_datetime(property_history_df['listedDate'])
    # Handling Numerics → integer or float
    property_history_df['daysOnMarket'] = property_history_df['daysOnMarket'].astype('Int64')
    # converting price to int by rounding off
    property_history_df['price'] = property_history_df['price'].round(0).astype('Int64')

    # Cleaning procedure for property_agent_df
    # Dropping any rows where sales_id is null
    property_agent_df.dropna(subset=['sales_id'], inplace=True)
    # Now property_agent_df has no rows where sales_id is null.
    # Filling the missing values
    property_agent_df['phone'].fillna('000-000-0000', inplace=True)
    property_agent_df['email'].fillna('unknown@example.com', inplace=True)
    # Trim whitespace & normalize casing
    property_agent_df['name']  = property_agent_df['name'].str.strip().str.title()
    property_agent_df['email'] = property_agent_df['email'].str.strip().str.lower()
    # Dealing with duplicates
    property_agent_df.drop_duplicates(subset=['sales_id'], keep='first')

    # Cleaning procedure for property_officer_df
    # Dropping any rows where sales_id is null
    property_officer_df.dropna(subset=['sales_id'], inplace=True)
    # Now property_officer_df has no rows where sales_id is null.
    # Drop redundant columns returning
    property_officer_df.drop(columns=["website"], inplace=True)
    # Filling the missing values
    property_officer_df['phone'].fillna('000-000-0000', inplace=True)
    property_officer_df['email'].fillna('unknown@example.com', inplace=True)
    # Trim whitespace & normalize casing
    property_officer_df['name']  = property_officer_df['name'].str.strip().str.title()
    property_officer_df['email'] = property_officer_df['email'].str.strip().str.lower()
    # Dealing withd duplicates
    property_officer_df.drop_duplicates(subset=['sales_id'], keep='first')

    # Cleaning procedure for property_sales_info_df
    # Converting to the right data type for strings and categorise
    property_sales_info_df['sales_id'] = property_sales_info_df['sales_id'].astype(str)
    property_sales_info_df['addressLine1'] = property_sales_info_df['addressLine1'].astype(str)
    property_sales_info_df['zipCode'] = property_sales_info_df['zipCode'].astype(str)
    property_sales_info_df['propertyType'] = property_sales_info_df['propertyType'].astype('category')
    # Drop redundant columns returning
    property_sales_info_df.drop(columns=["lotSize"], inplace=True)
    # Convert your float‐with‐NaNs column into a nullable integer column:
    property_sales_info_df['bedrooms'] = property_sales_info_df['bedrooms'].astype('Int64')
    property_sales_info_df['squareFootage'] = property_sales_info_df['squareFootage'].astype('Int64')
    property_sales_info_df['daysOnMarket'] = property_sales_info_df['daysOnMarket'].astype('Int64')
    property_sales_info_df['yearBuilt'] = property_sales_info_df['yearBuilt'].astype('Int64')
    # converting price to int by rounding off
    property_sales_info_df['price'] = property_sales_info_df['price'].round(0).astype('Int64')
    # Dealing with datetime
    property_sales_info_df['listedDate'] = pd.to_datetime(property_sales_info_df['listedDate'], utc=True, errors='coerce')
    # Address normalization
    property_sales_info_df['addressLine1'] = property_sales_info_df['addressLine1'].str.strip()
    property_sales_info_df['addressLine1'] = property_sales_info_df['addressLine1'].str.replace(r'\s+', ' ', regex=True)
    property_sales_info_df['addressLine1'] = property_sales_info_df['addressLine1'].str.title()
    # Dealing withd duplicates
    property_sales_info_df.drop_duplicates(subset=['sales_id'], keep='first')

    # Cleaning procedure for rental_history_df
    # Drop multiple columns, returning a new DataFrame
    rental_history_df.drop(columns=["event", "listingType", "removedDate"], inplace=True)
    # Dealing withd duplicates
    rental_history_df.drop_duplicates(subset=['rental_id'], keep='first')
    # Dealing with datetime
    rental_history_df['listedDate']  = pd.to_datetime(rental_history_df['listedDate'])
    # Handling Numerics → integer or float
    rental_history_df['price'] = rental_history_df['price'].astype('Int64')
    rental_history_df['daysOnMarket']= rental_history_df['daysOnMarket'].astype('Int64')

    # Cleaning procedure for rental_listing_info_df
    # Converting to the right data type for strings and categorise
    rental_listing_info_df['rental_id'] = rental_listing_info_df['rental_id'].astype(str)
    rental_listing_info_df['addressLine1'] = rental_listing_info_df['addressLine1'].astype(str)
    rental_listing_info_df['zipCode'] = rental_listing_info_df['zipCode'].astype(str)
    rental_listing_info_df['propertyType']  = rental_listing_info_df['propertyType'].astype('category')
    # Convert your float‐with‐NaNs column into a nullable integer column:
    rental_listing_info_df['bedrooms'] = rental_listing_info_df['bedrooms'].astype('Int64')
    rental_listing_info_df['squareFootage'] = rental_listing_info_df['squareFootage'].astype('Int64')
    rental_listing_info_df['daysOnMarket']  = rental_listing_info_df['daysOnMarket'].astype('Int64')
    # converting price to int by rounding off
    rental_listing_info_df['price'] = rental_listing_info_df['price'].round(0).astype('Int64')
    # Dealing with datetime
    rental_listing_info_df['listedDate'] = pd.to_datetime(rental_listing_info_df['listedDate'], utc=True, errors='coerce')
    # Address normalization
    rental_listing_info_df['addressLine1'] = rental_listing_info_df['addressLine1'].str.strip()
    rental_listing_info_df['addressLine1'] = rental_listing_info_df['addressLine1'].str.replace(r'\s+', ' ', regex=True)
    rental_listing_info_df['addressLine1'] = rental_listing_info_df['addressLine1'].str.title()
    # Dealing withd duplicates
    rental_listing_info_df.drop_duplicates(subset=['rental_id'], keep='first')
    
    
    # Last minute cleaning for property_sales_info_df
    # State code we want to replace using lookup dict
    lookup = {
        'WV': 'West Virginia',
        'VA': 'Virginia',
        'SC': 'South Carolina',
        'NC': 'North Carolina',
        'MD': 'Maryland',
        'GA': 'Georgia',
        'FL': 'Florida',
        'DE': 'Delaware'
}
    # overwrite the code column in-place
    property_sales_info_df['state'] = property_sales_info_df['state'].replace(lookup)

    # rename addressLine1 to address
    property_sales_info_df.rename(columns={'addressLine1': 'address'}, inplace=True)



    # Last minute cleaning for rental_listing_info_df
    # State code we want to replace using lookup dict
    lookup = {
        'WV': 'West Virginia',
        'VA': 'Virginia',
        'SC': 'South Carolina',
        'NC': 'North Carolina',
        'MD': 'Maryland',
        'GA': 'Georgia',
        'FL': 'Florida',
        'DE': 'Delaware'
    }
    # overwrite the code column in-place
    rental_listing_info_df['state'] = rental_listing_info_df['state'].replace(lookup)

    # rename addressLine1 to address
    rental_listing_info_df.rename(columns={'addressLine1': 'address'}, inplace=True)


    return {
        'property_history_df': property_history_df,
        'property_agent_df': property_agent_df,
        'property_officer_df': property_officer_df,
        'property_sales_info_df': property_sales_info_df,
        'rental_history_df': rental_history_df,
        'rental_listing_info_df': rental_listing_info_df
    }


# x = cleaning_job_2(all_data_22)
# print(x)