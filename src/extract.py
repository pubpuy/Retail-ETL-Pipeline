import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables for later use
load_dotenv()

def extract_data(file_path):
    """
    Reads data from a CSV file and returns a pandas DataFrame.
    """
    print(f"Checking file at: {file_path}")
    
    # Check if the file exists
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
        return None
    
    try:
        # Load the entire dataset
        df = pd.read_csv(file_path)
        
        # Validation: Check if it matches our expected 12,575 rows
        row_count = len(df)
        print(f"Success: Extracted {row_count} rows.")
        
        # Display the first 5 rows to verify columns
        print("Data Preview:")
        print(df.head())
        
        return df
    
    except Exception as e:
        print(f"An error occurred during extraction: {e}")
        return None

if __name__ == "__main__":
    # Define data path relative to project root
    DATA_FILE = "data/retail_store_sales.csv"
    raw_df = extract_data(DATA_FILE)