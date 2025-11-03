#!/usr/bin/env python3
"""
Test script for the customer validator
"""

import os
import sys
import pandas as pd
from datetime import datetime

# Add the current directory to the Python path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from processing.customer_validator import process_customer_file

def create_test_file():
    """Create a test Excel file with missing data"""
    
    # Create test data with missing values
    test_data = {
        'Customer: Email': ['test1@example.com', '', 'test3@example.com'],
        'Billing: First Name': ['John', '', 'Jane'],
        'Billing: Last Name': ['Doe', 'Smith', ''],
        'Billing: Phone': ['+1234567890', '', ''],
        'Billing: Address 1': ['123 Main St', '', '456 Oak Ave'],
        'Billing: Country Code': ['US', 'BG', ''],
        'Shipping: First Name': ['John', '', 'Jane'],
        'Shipping: Last Name': ['Doe', 'Smith', ''],
        'Shipping: Phone': ['', '+9876543210', ''],
        'Shipping: Address 1': ['123 Main St', '', '456 Oak Ave'],
        'Shipping: City': ['New York', '', 'Sofia'],
        'Shipping: Country Code': ['US', 'BG', '']
    }
    
    df = pd.DataFrame(test_data)
    test_file = 'test_customers.xlsx'
    
    # Save using openpyxl for better compatibility
    print(f"Creating test file: {test_file}")
    try:
        df.to_excel(test_file, index=False, engine='openpyxl')
        print(f"Test file created successfully with openpyxl")
        
        # Verify it can be read
        test_read = pd.read_excel(test_file, engine='openpyxl')
        print(f"Test file verification: {len(test_read)} rows readable")
        
    except Exception as e:
        print(f"Error creating test file: {e}")
        return None
        
    return test_file

def test_file_reading(file_path):
    """Test reading a file with different engines"""
    print(f"\nTesting file reading: {file_path}")
    
    engines = ['openpyxl', 'xlrd', None]  # None = default
    
    for engine in engines:
        try:
            if engine:
                df = pd.read_excel(file_path, engine=engine)
                print(f"✓ Engine '{engine}': {len(df)} rows, {len(df.columns)} columns")
            else:
                df = pd.read_excel(file_path)
                print(f"✓ Default engine: {len(df)} rows, {len(df.columns)} columns")
        except Exception as e:
            engine_name = engine if engine else 'default'
            print(f"✗ Engine '{engine_name}': {e}")

def main():
    print("Testing Shopify Customer Data Validator")
    print("=" * 50)
    
    # Test file reading if provided
    if len(sys.argv) > 1:
        test_file = sys.argv[1]
        if os.path.exists(test_file):
            print(f"Testing provided file: {test_file}")
            test_file_reading(test_file)
            
            print(f"\nProcessing provided file...")
            output_file = process_customer_file(test_file)
            
            if output_file:
                print(f"\nProcessing completed!")
                print(f"Output file: {output_file}")
                
                # Test reading the output file
                print(f"\nTesting output file readability...")
                test_file_reading(output_file)
            else:
                print(f"\nProcessing failed!")
        else:
            print(f"File not found: {test_file}")
    else:
        # Create and test with sample file
        test_file = create_test_file()
        
        if test_file:
            # Process the test file
            print(f"\nProcessing test file...")
            output_file = process_customer_file(test_file)
            
            if output_file:
                print(f"\nTest completed successfully!")
                print(f"Output file: {output_file}")
                
                # Test reading the output file
                print(f"\nTesting output file readability...")
                test_file_reading(output_file)
                
                # Clean up test files
                try:
                    os.remove(test_file)
                    os.remove(output_file)
                    print(f"\nCleaned up test files")
                except:
                    pass
            else:
                print(f"\nTest failed!")

if __name__ == "__main__":
    main()
