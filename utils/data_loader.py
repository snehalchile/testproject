import pandas as pd
import os

def load_csv(file_path):
    """Load data from a CSV file into a DataFrame."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")
    df = pd.read_csv(file_path)
    return df

def save_csv(file_path, data):
    """Save a DataFrame to a CSV file."""
    data.to_csv(file_path, index=False)
