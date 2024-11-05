# Data Synchronization and Update Process
import json
from utils.data_loader import load_csv, save_csv
from utils.error_handler import log_error, read_error_log
from utils.email_sender import email_error_log
import pandas as pd

def load_tables():
    try:
        input_table = business_table = pd.DataFrame()
        input_table = load_csv(f"file_base_db/case2_files/input_feed.csv")
        business_table = load_csv(f"file_base_db/case2_files/business_table.csv")
    except Exception as e:
        log_error(f"Error loading: {str(e)}")
    return input_table, business_table

def process_data(input_table, business_table):
    try:
        if business_table.empty:
            # If business_table is empty, all records from input_table are inserts
            business_table = input_table.copy()
            business_table['action'] = 'INSERT'
            update_df = pd.DataFrame()  # No updates if business_table is empty
            delete_df = pd.DataFrame()
        else:
            # All data that is exactly the same
            is_exact_df_match = input_table.equals(business_table)
            ''' This will update all action values to 'NC' since the DataFrames are identical. '''
            if is_exact_df_match:
                business_table['action'] = 'NC'
                # return
            
            # Identify new data in input_table which is not there in business_table
            records_not_in_business_table = input_table[~input_table['id'].isin(business_table['id'])]
            if not records_not_in_business_table.empty:
                records_not_in_business_table['action'] = 'INSERT'
                business_table = pd.concat([business_table, records_not_in_business_table], ignore_index=True)
            
            # Identify data difference and update values
            
            columns_to_compare = input_table.columns.difference(['action','id']).to_list()
            merged_df = input_table.merge(business_table, on='id', suffixes=('_input_table', '_business_table'))


            # Prepare the DataFrames for comparison
            input_cols = [col + '_input_table' for col in columns_to_compare]
            business_cols = [col + '_business_table' for col in columns_to_compare]

            # Ensure the comparison DataFrames are correctly aligned and labeled
            input_data = merged_df[input_cols]
            business_data = merged_df[business_cols]

            # Use the compare method to find differences
            try:
                diff = input_data.compare(business_data)
                print(diff)
            except ValueError as e:
                print(f"Comparison Error: {e}")

            merged_df = input_table.merge(
                business_table, on='id', how='left', suffixes=('', '_business')
            )
            breakpoint()

            # Identify INSERT and UPDATE records
            insert_df = merged_df[merged_df["id_business"].isna()].drop(columns=[col for col in merged_df.columns if '_business' in col])
            update_df = merged_df[~merged_df["id_business"].isna() & (merged_df.drop(columns=['id', f"id_business"]) != merged_df.filter(regex='_business$')).any(axis=1)]

            # Step 2: Perform an anti-join to find DELETE records
            delete_df = business_table[~business_table['id'].isin(input_table['id'])]

            # Output results
            if not insert_df.empty:
                print("INSERT Records:")
                print(insert_df)

            if not update_df.empty:
                print("UPDATE Records:")
                print(update_df)

            if not delete_df.empty:
                print("DELETE Records:")
                print(delete_df)

        # Optional: save results to CSV files
        insert_df.to_csv('insert_records.csv', index=False)
        update_df.to_csv('update_records.csv', index=False)
        delete_df.to_csv('delete_records.csv', index=False)
    except Exception as e:
        log_error(f"Error loading: {str(e)}")


def run():
    """Main execution function for Data Synchronization and Update Process."""
    try:
        input_table, business_table = load_tables()
        print("input_table",input_table)
        print("business_table",business_table)
        process_data(input_table, business_table)
    except Exception as e:
        log_error(f"Error in Data Synchronization and Update Process: {str(e)}")

if __name__ == "__main__":
    run()
