import json
import os

def load_config(config_file='config.json'):
    """Load configuration from a JSON file."""
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"The configuration file {config_file} does not exist.")
    
    with open(config_file, 'r') as file:
        return json.load(file)
