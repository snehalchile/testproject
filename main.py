import case_study_1
import case_study_2
from utils.config_loader import load_config
config = load_config()

def run_case_study(choice):
    if choice == 1:
        case_study_1.run()
    elif choice == 2:
        case_study_2.run()
    else:
        print("Invalid choice.")

if __name__ == "__main__":
    print("Select a case study to run:")
    print("1: Dynamic Personnel Data Management System")
    print("2: Data Synchronization and Update Process")
    
    try:
        choice = int(input("Enter your choice (1 or 2): "))
        run_case_study(choice)
    except ValueError:
        print("Please enter a valid number.")
