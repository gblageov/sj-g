import pandas as pd
import os
from datetime import datetime

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
        # Read the Excel file
        print(f"Reading file: {input_file}")
        
        # Try different engines to read the file
        try:
            df = pd.read_excel(input_file, engine='openpyxl')
            print(f"File read successfully using openpyxl engine")
        except Exception as e1:
            print(f"Failed to read with openpyxl: {e1}")
            try:
                df = pd.read_excel(input_file, engine='xlrd')
                print(f"File read successfully using xlrd engine")
            except Exception as e2:
                print(f"Failed to read with xlrd: {e2}")
                try:
                    df = pd.read_excel(input_file)  # Default engine
                    print(f"File read successfully using default engine")
                except Exception as e3:
                    print(f"Failed to read with default engine: {e3}")
                    raise Exception(f"Unable to read Excel file: {e3}")
        
        print(f"File loaded successfully. Total rows: {len(df)}")
        print(f"Columns found: {list(df.columns)}")
        
        # Define required fields for Shopify
        required_fields = [
            'Customer: Email',
            'Billing: First Name', 
            'Billing: Last Name',
            'Billing: Phone',
            'Billing: Address 1',
            'Billing: Country Code',  # Priority over 'Billing: Country'
            'Billing: Country',       # Fallback if Country Code is missing
            'Shipping: First Name',
            'Shipping: Last Name', 
            'Shipping: Phone',
            'Shipping: Address 1',
            'Shipping: City',
            'Shipping: Country Code'
        ]
        
        # Check which required columns exist in the dataframe
        existing_columns = [col for col in required_fields if col in df.columns]
        missing_columns = [col for col in required_fields if col not in df.columns]
        
        print(f"\nExisting required columns: {len(existing_columns)}")
        for col in existing_columns:
            print(f"  - {col}")
            
        if missing_columns:
            print(f"\nMissing required columns: {len(missing_columns)}")
            for col in missing_columns:
                print(f"  - {col}")
        
        # Find rows with missing data in required fields
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
        
        # Fix missing data
        print(f"\nFixing missing data...")
        fixed_count = 0
        
        # Generate column statistics before fixing
        print(f"\n" + "=" * 50)
        print("COLUMN STATISTICS BEFORE FIXING")
        print("=" * 50)
        
        for col in existing_columns:
            filled_count = df[col].notna().sum() - (df[col] == '').sum()
            missing_count = df[col].isna().sum() + (df[col] == '').sum()
            print(f"{col} - {filled_count} filled fields, {missing_count} missing fields")
        
        for col in existing_columns:
            missing_before = df[col].isna().sum() + (df[col] == '').sum()
            
            if col == 'Customer: Email':
                # Fill email field with "shopify@getnada.com"
                df[col] = df[col].fillna('shopify@getnada.com')
                df[col] = df[col].replace('', 'shopify@getnada.com')
            elif 'Phone' in col:
                # Fill phone fields with "+1234567890"
                df[col] = df[col].fillna('+1234567890')
                df[col] = df[col].replace('', '+1234567890')
            else:
                # Fill other fields with "Shopify"
                df[col] = df[col].fillna('Shopify')
                df[col] = df[col].replace('', 'Shopify')
            
            missing_after = df[col].isna().sum() + (df[col] == '').sum()
            if missing_before > 0:
                print(f"  Fixed {col}: {missing_before} missing values -> {missing_after} missing")
                fixed_count += missing_before
        
        print(f"\nTotal fixed values: {fixed_count}")
        
        # Generate column statistics after fixing
        print(f"\n" + "=" * 50)
        print("COLUMN STATISTICS AFTER FIXING")
        print("=" * 50)
        
        for col in existing_columns:
            filled_count = df[col].notna().sum() - (df[col] == '').sum()
            missing_count = df[col].isna().sum() + (df[col] == '').sum()
            print(f"{col} - {filled_count} filled fields, {missing_count} missing fields")
        
        # Verify no missing data remains in required fields
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
        
        # Generate output filename if not provided
        if output_file is None:
            base_name = os.path.splitext(input_file)[0]
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"{base_name}_{timestamp}.xlsx"
        
        # Save the fixed file
        print(f"\nSaving fixed file to: {output_file}")
        print("Please wait... Processing and saving the file (this may take a moment for large files)")
        
        # Use openpyxl engine for better compatibility
        try:
            # Save with custom sheet name "Orders"
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Orders')
            print(f"File saved successfully using openpyxl engine with sheet name 'Orders'")
            
            # Verify the file was created and is readable
            if os.path.exists(output_file):
                file_size = os.path.getsize(output_file)
                print(f"File size: {file_size} bytes")
                
                # Try to read it back to verify it's valid
                try:
                    test_df = pd.read_excel(output_file, engine='openpyxl')
                    print(f"File verification successful - {len(test_df)} rows readable")
                except Exception as verify_e:
                    print(f"Warning: File verification failed: {verify_e}")
            else:
                print("Error: Output file was not created")
                return None
                
        except Exception as e:
            print(f"Error saving with openpyxl: {e}")
            # Fallback to default engine
            try:
                df.to_excel(output_file, index=False)
                print(f"File saved using default engine")
            except Exception as fallback_e:
                print(f"Error saving with default engine: {fallback_e}")
                return None
        
        # Generate summary report
        print(f"\n" + "=" * 50)
        print("SUMMARY REPORT")
        print("=" * 50)
        print(f"Input file: {input_file}")
        print(f"Output file: {output_file}")
        print(f"Total rows processed: {len(df)}")
        print(f"Required columns found: {len(existing_columns)}")
        print(f"Required columns missing: {len(missing_columns)}")
        print(f"Rows with missing data: {len(rows_with_missing)}")
        print(f"Total values fixed: {fixed_count}")
        print(f"Remaining missing values: {remaining_missing}")
        
        if missing_columns:
            print(f"\nWARNING: The following required columns were not found in the file:")
            for col in missing_columns:
                print(f"  - {col}")
            print("You may need to add these columns manually for Shopify import.")
        
        return output_file
        
    except FileNotFoundError:
        print(f"Error: Input file not found: {input_file}")
        return None
    except PermissionError:
        print(f"Error: Permission denied when accessing file: {input_file}")
        return None
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        return None
