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
    """Create a test Excel file with an Orders sheet and Woo metafield phones"""
    test_file = 'test_customers.xlsx'

    # Minimal Orders sheet including Top Row, Name, and phone fields
    orders_data = {
        'Top Row': ['1', '', ''],
        'Name': ['#1001', '#1001', '#1001'],
        'Customer: Email': ['top@example.com', '', ''],
        'Customer: Phone': ['', '', ''],
        'Billing: First Name': ['Top', '', ''],
        'Billing: Last Name': ['Row', '', ''],
        'Billing: Phone': ['', '', ''],
        'Shipping: Phone': ['', '', ''],
        'Metafield: woo._billing_tel': ['+359888000111', '', ''],
        'Metafield: woo.billing_tel': ['', '', ''],
        'Billing: Address 1': ['Addr', '', ''],
        'Billing: City': ['Sofia', '', ''],
        'Billing: Country Code': ['BG', '', ''],
        'Billing: Country': ['Bulgaria', '', ''],
        'Shipping: First Name': ['', '', ''],
        'Shipping: Last Name': ['', '', ''],
        'Shipping: Address 1': ['', '', ''],
        'Shipping: City': ['', '', ''],
        'Shipping: Country': ['', '', ''],
        'Shipping: Country Code': ['', '', ''],
        'Command': ['NEW', 'NEW', 'NEW']
    }
    df_orders = pd.DataFrame(orders_data)

    print(f"Creating test file with Orders sheet: {test_file}")
    try:
        with pd.ExcelWriter(test_file, engine='openpyxl') as writer:
            df_orders.to_excel(writer, index=False, sheet_name='Orders')
        print("Test file created successfully with Orders sheet")
        # Verify
        xl = pd.ExcelFile(test_file, engine='openpyxl')
        print(f"Sheets: {xl.sheet_names}")
        test_read = pd.read_excel(test_file, sheet_name='Orders', engine='openpyxl')
        print(f"Orders rows readable: {len(test_read)}")
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
                
                # Optional: keep files for manual inspection
            else:
                print(f"\nTest failed!")

if __name__ == "__main__":
    main()
