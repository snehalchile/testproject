#Dynamic Personnel Data Management System
import json
from utils.data_loader import load_csv, save_csv
from utils.error_handler import log_error, read_error_log
from utils.email_sender import email_error_log
from main import config


def load_tables(config):
    """Load personnel data tables from CSV files specified in the configuration."""
    personnel_data = {}
    
    for table in config['personnel_tables']:
        table_name = table['name']
        try:
            personnel_data[table_name] = load_csv(f"file_base_db/{table_name}.csv")
        except Exception as e:
            log_error(f"Error loading {table_name}: {str(e)}")
    
    return personnel_data

def validate_personnel(personnel_data):
    """Validate personnel data for department associations."""
    errors = []

    # Validate employee DataFrame
    employee_data = personnel_data.get("employee")
    if employee_data is None or employee_data.empty:
        errors.append("No employee data available.")
    else:
        # Check for missing DepartmentID
        missing_department_ids = employee_data[employee_data["DepartmentID"].isna()]
        for _, personnel in missing_department_ids.iterrows():
            errors.append(f"Employee ID {personnel['Employee ID']} has no associated department.")

    # Validate department DataFrame
    department_data = personnel_data.get("department")
    if department_data is None or department_data.empty:
        errors.append("No department data available.")
    else:
        # Check for missing leads
        missing_leads = department_data[department_data["Lead"].isna()]
        for _, department in missing_leads.iterrows():
            errors.append(f"Department ID {department['DepartmentID']} has no assigned lead.")
    
    return errors

def run():
    """Main execution function for the personnel data management system."""
    try:
        # Load personnel data tables
        personnel_data = load_tables(config)
        
        # Validate personnel data
        errors = validate_personnel(personnel_data)
        
        # Log errors and send email if any
        if errors:
            for error in errors:
                log_error(error)
            email_error_log("test@gmail.com") 
        print("Personnel data management completed. Check the error log for details.")
        
    except Exception as e:
        log_error(f"Error in personnel data management: {str(e)}")

if __name__ == "__main__":
    run()
