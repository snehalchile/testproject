# Data Synchronization and Update Process
import json
from utils.data_loader import load_csv, save_csv
from utils.error_handler import log_error, read_error_log
from utils.email_sender import email_error_log
import pandas as pd

def load_tables(path):
    try:
        input_table = business_table = pd.DataFrame()
        input_table = load_csv(f"file_base_db/case2_files/{path}/input_feed.csv")
        business_table = load_csv(f"file_base_db/case2_files/{path}/business_table.csv")
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
                business_table['action'] = 'No Change'
            
            # Identify data difference and update values
            new_rows = input_table[~input_table['id'].isin(business_table['id'])].copy()
            new_rows['action'] = 'Insert'
            common_rows = pd.merge(input_table, business_table, on='id', suffixes=('_input_table', '_business_table'))
            diff_columns = [col for col in input_table.columns if col != 'id']
            updated_rows = common_rows[
                (common_rows[[f"{col}_input_table" for col in diff_columns]].values != common_rows[[f"{col}_business_table" for col in diff_columns]].values).any(axis=1)
            ]
            updates = updated_rows[['id'] + [f"{col}_input_table" for col in diff_columns]].copy()
            updates.columns = ['id'] + diff_columns  # Rename columns to remove the '_df1' suffix
            updates['action'] = 'Update'

            # Step 3: Find rows with no changes
            no_change_rows = common_rows[
                (common_rows[[f"{col}_input_table" for col in diff_columns]].values == common_rows[[f"{col}_business_table" for col in diff_columns]].values).all(axis=1)
            ]
            # no_changes = no_change_rows[['id'] + [f"{col}_input_table" for col in diff_columns]].copy()

            no_change_ids = no_change_rows['id']
            
            not_exist_in_current_data = business_table[~business_table['id'].isin(input_table['id'])]['id'].to_list()
            business_table.loc[business_table['id'].isin(not_exist_in_current_data),'action'] = 'Delete'

            business_table.set_index('id', inplace=True)
            updates.set_index('id', inplace=True)

            business_table.update(updates)
            business_table.loc[updates.index, 'action'] = 'Update'

            business_table.loc[no_change_ids, 'action'] = 'No Change'

            # Reset the index after updating
            business_table.reset_index(inplace=True)
            business_table = pd.concat([business_table, new_rows], ignore_index=True)
            
        #save results to CSV files
        business_table.to_csv('business_table.csv', index=False)
        input_table.to_csv('input_table.csv', index=False)
    except Exception as e:
        log_error(f"Error loading: {str(e)}")

def example_test(choice):
    if choice == 1:
        path = 'run1_insert'
    elif choice == 2:
        path = 'run2_update'
    elif choice == 3:
        path = 'run3_delete'
    elif choice == 3:
        path = 'run4_no_change'
    else:
        print("Invalid choice.")
        path = None
    return path

def run():
    """Main execution function for Data Synchronization and Update Process."""
    print("Select example to test:")
    print("1: Insert data")
    print("2: Update data")
    print("3. Delete data")
    print("4. No change data")
    
    try:
        choice = int(input("Enter your choice (1 or 2): "))
        path = example_test(choice)
    except ValueError:
        print("Please enter a valid number.")

    try:
        if path:
            input_table, business_table = load_tables(path)
            print("input_table",input_table)
            print("business_table",business_table)
            process_data(input_table, business_table)
        else: 
            print("Please select choices..")
    except Exception as e:
        log_error(f"Error in Data Synchronization and Update Process: {str(e)}")

if __name__ == "__main__":
    run()
