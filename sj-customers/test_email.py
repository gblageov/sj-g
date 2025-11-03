#!/usr/bin/env python3
"""
Test script to verify email filling
"""

import os
import sys
import pandas as pd

# Add the current directory to the Python path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from processing.customer_validator import process_customer_file

def test_email_filling():
    """Create a test file specifically for email testing"""
    
    # Create test data with missing email
    test_data = {
        'Customer: Email': ['existing@email.com', '', 'another@test.com'],
        'Billing: First Name': ['John', 'Jane', 'Bob'],
        'Billing: Last Name': ['Doe', 'Smith', 'Brown'],
        'Billing: Phone': ['+1234567890', '+9876543210', '+5555555555'],
        'Billing: Address 1': ['123 Main St', '456 Oak Ave', '789 Pine Rd'],
        'Billing: Country Code': ['US', 'BG', 'UK'],
        'Shipping: First Name': ['John', 'Jane', 'Bob'],
        'Shipping: Last Name': ['Doe', 'Smith', 'Brown'],
        'Shipping: Phone': ['+1234567890', '+9876543210', '+5555555555'],
        'Shipping: Address 1': ['123 Main St', '456 Oak Ave', '789 Pine Rd'],
        'Shipping: City': ['New York', 'Sofia', 'London'],
        'Shipping: Country Code': ['US', 'BG', 'UK']
    }
    
    df = pd.DataFrame(test_data)
    test_file = 'test_email_filling.xlsx'
    
    print("Creating test file for email verification...")
    df.to_excel(test_file, index=False, engine='openpyxl')
    
    print("\nOriginal data:")
    print(df['Customer: Email'].tolist())
    
    # Process the file
    print(f"\nProcessing test file...")
    output_file = process_customer_file(test_file)
    
    if output_file:
        # Read the output and check emails
        result_df = pd.read_excel(output_file, engine='openpyxl')
        print(f"\nResult data:")
        print(result_df['Customer: Email'].tolist())
        
        # Check specific values
        emails = result_df['Customer: Email'].tolist()
        print(f"\nEmail verification:")
        for i, email in enumerate(emails, 1):
            print(f"Row {i}: '{email}'")
        
        # Clean up
        try:
            os.remove(test_file)
            os.remove(output_file)
        except:
            pass
    else:
        print("Processing failed!")

if __name__ == "__main__":
    test_email_filling()
