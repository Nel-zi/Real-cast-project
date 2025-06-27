import json
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from cleaning_job import cleaning_job_2

with open('all_data_22.json', 'r', encoding='utf-8') as f:
    all_data_22 = json.load(f)

def load_data(all_data_22):
    # 1. Call the function and grab its output
    cleaned_3 = cleaning_job_2(all_data_22)

    # 2. Extract each list by key
    property_history_df         = cleaned_3["property_history_df"]
    property_agent_df           = cleaned_3["property_agent_df"]
    property_officer_df         = cleaned_3["property_officer_df"]
    property_sales_info_df      = cleaned_3["property_sales_info_df"]
    rental_history_df           = cleaned_3["rental_history_df"]
    rental_listing_info_df      = cleaned_3["rental_listing_info_df"]

    # Load your .env
    load_dotenv()  # looks for a .env file in cwd

    DB_USER     = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST     = os.getenv("DB_HOST", "localhost")
    DB_PORT     = os.getenv("DB_PORT", "5432")
    DB_NAME     = os.getenv("DB_NAME")

    # Create the engine
    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    # Prepare your DataFrames and target table names
    tables = {
        "sales_info": property_sales_info_df,
        "sales_history": property_history_df,
        "sales_agent": property_agent_df,
        "sales_officer": property_officer_df,
        "rental_history": rental_history_df,
        "rental_info": rental_listing_info_df
    }

    # Loop and write each one
    for table_name, df in tables.items():
        df.to_sql(
            name=table_name,
            con=engine,
            schema="public",         # adjust if you use another schema
            if_exists="replace",     # or "append" There are risk to using append for future batch jobs, like if it appends PK i already have in there, it'll break.
            index=False,             # drop the DataFrame’s index column
            chunksize=500            # adjust batch size to manage memory/performance
        )
        print(f"→ Loaded {len(df)} rows into {table_name}")


load_data(all_data_22)


# # run_job
# if __name__ == "__main__":
#     # # 1) extract your raw JSON‐like payload
#     # all_data = data_extraction()

#     # 2) hand it off to your loader
#     load_data(all_data)

#     print("✅ All tables written to the database.")
