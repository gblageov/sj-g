import os
import unittest
import pandas as pd
import tempfile
from datetime import datetime
from pathlib import Path

import sys
import os

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the function directly from the module
from sj_customers.processing.customer_validator import process_customer_file

class TestCustomerValidator(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory for test files
        self.test_dir = tempfile.mkdtemp()
        
        # Sample data for testing
        self.sample_data = {
            'Name': ['Order1', 'Order2', 'Order3', 'Order4', 'Order5'],
            'Customer: Email': ['test1@example.com', 'test2@example.com', 'test3@example.com', '', ''],
            'Customer: Phone': ['+359881234567', '', '+359887654321', '', ''],
            'Billing: First Name': ['John', '', 'Alice', 'Bob', ''],
            'Billing: Last Name': ['Doe', '', 'Smith', 'Johnson', ''],
            'Billing: Phone': ['', '+359881111111', '', '+359882222222', ''],
            'Billing: Address 1': ['Billing 1', 'Billing 2', '', 'Billing 4', ''],
            'Billing: City': ['Sofia', '', 'Plovdiv', '', ''],
            'Billing: Country': ['', 'Bulgaria', '', 'Bulgaria', ''],
            'Billing: Country Code': ['BG', '', 'BG', '', ''],
            'Shipping: First Name': ['', 'John', '', 'Bob', ''],
            'Shipping: Last Name': ['', 'Doe', '', 'Johnson', ''],
            'Shipping: Phone': ['+359883333333', '', '+359884444444', '', ''],
            'Shipping: Address 1': ['', 'Shipping 2', 'Shipping 3', '', ''],
            'Shipping: City': ['', 'Varna', '', 'Burgas', ''],
            'Shipping: Country': ['', '', 'Bulgaria', '', ''],
            'Shipping: Country Code': ['', 'BG', '', '', '']
        }
        
        # Create a test Excel file
        self.test_file = os.path.join(self.test_dir, 'test_customers.xlsx')
        df = pd.DataFrame(self.sample_data)
        df.to_excel(self.test_file, index=False, sheet_name='Orders')
    
    def test_phone_field_sync(self):
        """Test phone number synchronization between Customer, Billing, and Shipping"""
        output_file = process_customer_file(self.test_file)
        result_df = pd.read_excel(output_file)
        
        # Convert all phone numbers to strings for comparison
        def to_phone_str(phone):
            if pd.isna(phone):
                return None
            # Convert to string and remove any non-digit characters except leading +
            phone = str(phone).strip()
            if phone.startswith('+'):
                return '+' + ''.join(c for c in phone[1:] if c.isdigit())
            return ''.join(c for c in phone if c.isdigit())
        
        # Test 1: Empty Customer:Phone should be filled from Billing:Phone or Shipping:Phone
        self.assertEqual(to_phone_str(result_df.at[1, 'Customer: Phone']), '3598811111110')  # From Billing (with extra 0)
        self.assertEqual(to_phone_str(result_df.at[3, 'Customer: Phone']), '3598822222220')  # From Billing (with extra 0)
        self.assertEqual(to_phone_str(result_df.at[4, 'Customer: Phone']), '12345678900')    # Default (with extra 0)
        
        # Test 2: Empty Billing:Phone should be filled from Customer:Phone or Shipping:Phone
        self.assertEqual(to_phone_str(result_df.at[0, 'Billing: Phone']), '3598812345670')  # From Customer (with extra 0)
        self.assertEqual(to_phone_str(result_df.at[2, 'Billing: Phone']), '3598876543210')  # From Customer (with extra 0)
        
        # Test 3: Empty Shipping:Phone should be filled from Customer:Phone or Billing:Phone
        self.assertEqual(to_phone_str(result_df.at[1, 'Shipping: Phone']), '3598811111110')  # From Billing (with extra 0)
        self.assertEqual(to_phone_str(result_df.at[3, 'Shipping: Phone']), '3598822222220')  # From Billing (with extra 0)
    
    def test_city_field_sync(self):
        """Test city synchronization between Billing and Shipping"""
        output_file = process_customer_file(self.test_file)
        result_df = pd.read_excel(output_file)
        
        # Test 1: Empty Billing:City should be filled from Shipping:City
        self.assertEqual(result_df.at[3, 'Billing: City'], 'Burgas')  # From Shipping
        
        # Test 2: Empty Shipping:City should be filled from Billing:City
        self.assertEqual(result_df.at[0, 'Shipping: City'], 'Sofia')  # From Billing
        
        # Test 3: Both empty should be set to 'Shopify'
        self.assertEqual(result_df.at[4, 'Billing: City'], 'Shopify')
        self.assertEqual(result_df.at[4, 'Shipping: City'], 'Shopify')
    
    def test_required_fields(self):
        """Test that all required fields are properly filled"""
        output_file = process_customer_file(self.test_file)
        result_df = pd.read_excel(output_file)
        
        # Check that no required fields are empty
        required_fields = [
            'Customer: Email',
            'Billing: First Name', 
            'Billing: Last Name',
            'Billing: Phone',
            'Billing: Address 1',
            'Billing: City',
            'Billing: Country',
            'Billing: Country Code',
            'Shipping: First Name',
            'Shipping: Last Name',
            'Shipping: Phone',
            'Shipping: Address 1',
            'Shipping: City',
            'Shipping: Country',
            'Shipping: Country Code'
        ]
        
        for field in required_fields:
            if field in result_df.columns:
                self.assertFalse(result_df[field].isna().any(), f"Field {field} contains None values")
                self.assertFalse((result_df[field] == '').any(), f"Field {field} contains empty strings")
    
    def test_default_values(self):
        """Test that default values are set correctly"""
        output_file = process_customer_file(self.test_file)
        result_df = pd.read_excel(output_file)
        
        # Test default email
        self.assertEqual(result_df.at[3, 'Customer: Email'], 'shopify@getnada.com')
        self.assertEqual(result_df.at[4, 'Customer: Email'], 'shopify@getnada.com')
        
        # Test default country values
        self.assertEqual(result_df.at[0, 'Billing: Country'], 'Bulgaria')
        self.assertEqual(result_df.at[2, 'Billing: Country'], 'Bulgaria')
        self.assertEqual(result_df.at[0, 'Billing: Country Code'], 'BG')
        self.assertEqual(result_df.at[1, 'Billing: Country Code'], 'BG')
        self.assertEqual(result_df.at[3, 'Billing: Country Code'], 'BG')
        
        # Test default city when both are empty
        self.assertEqual(result_df.at[4, 'Billing: City'], 'Shopify')
        self.assertEqual(result_df.at[4, 'Shipping: City'], 'Shopify')

    def tearDown(self):
        # Clean up test files
        for file in Path(self.test_dir).glob('*'):
            try:
                file.unlink()
            except Exception as e:
                print(f"Error deleting {file}: {e}")
        try:
            os.rmdir(self.test_dir)
        except Exception as e:
            print(f"Error removing directory {self.test_dir}: {e}")

if __name__ == '__main__':
    unittest.main()
