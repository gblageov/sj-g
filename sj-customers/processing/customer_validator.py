"""
Customer Validator - Main Module
Orchestrates the validation and fixing of customer data for Shopify import.
"""

import pandas as pd
import os
import sys

# Add processing directory to path for direct imports
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

import order_processor
import field_validators
import file_handler

# Import specific functions
from order_processor import get_order_groups, propagate_order_data
from field_validators import (
    get_existing_required_columns, 
    process_all_fields, 
    add_judgeme_tag,
    REQUIRED_FIELDS
)
from file_handler import (
    read_excel_file,
    normalize_dataframe,
    truncate_by_empty_name,
    save_excel_file,
    print_column_statistics,
    print_summary_report
)


def process_customer_file(input_file, output_file=None):
    """
    Process customer data file to validate and fix missing required fields for Shopify import.
    
    Args:
        input_file (str): Path to input Excel file
        output_file (str): Path to output Excel file (optional)
    
    Returns:
        str: Path to output file if successful, None otherwise
    """
    
    try:
        # Step 1: Read Excel file
        df, sheet_names = read_excel_file(input_file)
        if df is None:
            return None
        
        # Step 2: Normalize data (Command column, TRUE/FALSE values)
        df = normalize_dataframe(df)
        
        # Step 3: Process order groups if Top Row column exists
        if 'Top Row' in df.columns:
            order_groups = get_order_groups(df)
            if order_groups:
                print(f"Processing {len(order_groups)} order groups...")
                df = propagate_order_data(df, order_groups)
        
        # Step 4: Truncate by empty Name column
        df = truncate_by_empty_name(df)
        
        print(f"File loaded successfully. Total rows: {len(df)}")
        print(f"Columns found: {list(df.columns)}")
        
        # Step 5: Ensure shipping country columns exist
        for col in ['Shipping: Country', 'Shipping: Country Code']:
            if col not in df.columns:
                df[col] = ''
                print(f"Added missing column: {col}")
        
        # Step 6: Check which required columns exist
        existing_columns, missing_columns = get_existing_required_columns(df)
        
        print(f"\nExisting required columns: {len(existing_columns)}")
        for col in existing_columns:
            print(f"  - {col}")
            
        if missing_columns:
            print(f"\nMissing required columns: {len(missing_columns)}")
            for col in missing_columns:
                print(f"  - {col}")
        
        # Step 7: Find rows with missing data
        print(f"\nChecking for missing data in required fields...")
        
        rows_with_missing = []
        for idx, row in df.iterrows():
            missing_fields = []
            for col in existing_columns:
                if pd.isna(row[col]) or str(row[col]).strip() == '':
                    missing_fields.append(col)
            
            if missing_fields:
                rows_with_missing.append({
                    'row_index': idx,
                    'missing_fields': missing_fields
                })
        
        print(f"Found {len(rows_with_missing)} rows with missing required data")
        
        if rows_with_missing:
            print("\nRows with missing data:")
            for row_info in rows_with_missing[:10]:  # Show first 10
                print(f"  Row {row_info['row_index'] + 1}: Missing {len(row_info['missing_fields'])} fields")
                for field in row_info['missing_fields']:
                    print(f"    - {field}")
            
            if len(rows_with_missing) > 10:
                print(f"  ... and {len(rows_with_missing) - 10} more rows")
        
        # Step 8: Print statistics before fixing
        print(f"\nFixing missing data...")
        print_column_statistics(df, existing_columns, "COLUMN STATISTICS BEFORE FIXING")
        
        # Step 9: Process and fix all fields
        df, fixed_count = process_all_fields(df, existing_columns)
        
        print(f"\nTotal fixed values: {fixed_count}")
        
        # Step 10: Print statistics after fixing
        print_column_statistics(df, existing_columns, "COLUMN STATISTICS AFTER FIXING")
        
        # Step 11: Verify no missing data remains
        print(f"\nVerifying fixed data...")
        remaining_missing = 0
        for col in existing_columns:
            missing = df[col].isna().sum() + (df[col] == '').sum()
            if missing > 0:
                print(f"  {col}: {missing} still missing")
                remaining_missing += missing
        
        if remaining_missing == 0:
            print("  All required fields are now complete!")
        else:
            print(f"  Warning: {remaining_missing} values still missing")
        
        # Step 12: Add judgeme_excluded tag
        df = add_judgeme_tag(df)
        
        # Step 13: Save the file
        output_file = save_excel_file(df, input_file, output_file)
        if output_file is None:
            return None
        
        # Step 14: Print summary report
        print_summary_report(input_file, output_file, df, existing_columns, 
                           missing_columns, rows_with_missing, fixed_count, remaining_missing)
        
        return output_file
        
    except FileNotFoundError:
        print(f"Error: Input file not found: {input_file}")
        return None
    except PermissionError:
        print(f"Error: Permission denied when accessing file: {input_file}")
        return None
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
        result = process_customer_file(input_file)
        if result:
            print(f"\nProcessing completed successfully!")
            print(f"Output file: {result}")
        else:
            print("\nProcessing failed!")
            sys.exit(1)
    else:
        print("Usage: python customer_validator.py <input_file.xlsx>")
        sys.exit(1)
