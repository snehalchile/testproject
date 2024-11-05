import os

def log_error(error_message, error_file='error_log.txt'):
    """Log an error message to the specified error file."""
    with open(error_file, mode='a') as file:
        file.write(error_message + '\n')

def read_error_log(error_file='error_log.txt'):
    """Read and return the contents of the error log."""
    if not os.path.exists(error_file):
        return []
    
    with open(error_file, mode='r') as file:
        return file.readlines()
