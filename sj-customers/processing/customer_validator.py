import pandas as pd
import os
from datetime import datetime
from typing import Dict, Any, List

def get_order_groups(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """
    Group rows by order and identify the top row for each order.
    
    Args:
        df: Input DataFrame containing order data
        
    Returns:
        Dictionary with order names as keys and order group info as values
    """
    if 'Top Row' not in df.columns or 'Name' not in df.columns:
        return {}
        
    order_groups = {}
    # Work with trimmed string versions of key columns to ensure robust matching
    name_series = df['Name'].astype(str).str.strip() if 'Name' in df.columns else pd.Series([], dtype=str)
    top_row_series = df['Top Row'].astype(str).str.strip() if 'Top Row' in df.columns else pd.Series([], dtype=str)
    top_rows = df[(top_row_series.notna()) & (top_row_series != '')]
    
    print(f"Found {len(top_rows)} orders with 'Top Row' values")
    
    for _, top_row in top_rows.iterrows():
        order_name = str(top_row['Name']).strip()
        if pd.isna(order_name) or order_name == '':
            continue
            
        # Find all rows with the same order name
        order_rows = df[name_series == order_name].index.tolist()
        
        order_groups[order_name] = {
            'top_row_index': top_row.name,  # index of the top row
            'row_indices': order_rows       # all row indices in this order
        }
    
    return order_groups

def propagate_order_data(df: pd.DataFrame, order_groups: Dict[str, Dict[str, Any]]) -> pd.DataFrame:
    """
    Propagate data from top row to other rows in the same order group.
    Only fills empty fields in target rows.
    
    Args:
        df: Input DataFrame
        order_groups: Dictionary of order groups from get_order_groups()
        
    Returns:
        Modified DataFrame with propagated data
    """
    if not order_groups:
        return df
        
    fields_to_propagate = [
        'Customer: Email', 'Customer: First Name', 'Customer: Last Name',
        'Billing: First Name', 'Billing: Last Name',
        'Billing: Address 1', 'Billing: City',
        'Billing: Country', 'Billing: Country Code',
        'Shipping: First Name', 'Shipping: Last Name',
        'Shipping: Address 1', 'Shipping: City', 'Shipping: Country',
        'Shipping: Country Code'
    ]
    
    # Phone handling: derive from any phone-like column on Top Row and propagate to standard phone fields
    standard_phone_fields = [ 'Billing: Phone', 'Shipping: Phone']
    all_phone_cols = standard_phone_fields + [
        'Metafield: woo._billing_tel',
        'Metafield: woo.billing_tel'
    ]
    
    propagated_count = 0
    propagated_phone_rows = 0
    
    for order_name, group in order_groups.items():
        top_row_idx = group['top_row_index']
        top_row = df.loc[top_row_idx]
        
        # Find a phone value on the Top Row from any known phone column
        top_phone = None
        for pcol in all_phone_cols:
            if pcol in df.columns:
                val = top_row.get(pcol)
                if isinstance(val, str):
                    val = val.strip()
                if val not in [None, ''] and not pd.isna(val):
                    top_phone = val
                    break
        
        # First propagate non-phone fields from Top Row
        for row_idx in group['row_indices']:
            if row_idx == top_row_idx:
                continue  # Skip the top row itself
            for field in fields_to_propagate:
                if field in df.columns and field in top_row:
                    target_val = df.at[row_idx, field] if field in df.columns else None
                    source_val = top_row[field]
                    if isinstance(target_val, str):
                        target_val = target_val.strip()
                    if isinstance(source_val, str):
                        source_val = source_val.strip()
                    # Only fill if target is empty and source has a value
                    if (target_val in [None, ''] or pd.isna(target_val)) and (source_val not in [None, ''] and not pd.isna(source_val)):
                        df.at[row_idx, field] = source_val
                        propagated_count += 1
        
        # Then propagate phone from Top Row to standard phone fields for the group
        if top_phone:
            for row_idx in group['row_indices']:
                if row_idx == top_row_idx:
                    continue
                for phone_field in standard_phone_fields:
                    if phone_field in df.columns:
                        current_val = df.at[row_idx, phone_field]
                        current_val_stripped = current_val.strip() if isinstance(current_val, str) else current_val
                        if (current_val_stripped in [None, ''] or pd.isna(current_val_stripped)):
                            df.at[row_idx, phone_field] = top_phone
                            propagated_phone_rows += 1
    
    if propagated_count > 0:
        print(f"Propagated {propagated_count} non-phone field values from top rows to order items")
    if propagated_phone_rows > 0:
        print(f"Propagated phone to {propagated_phone_rows} positions from Top Rows")
    
    return df

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
        
        # Get all sheet names first
        try:
            xl = pd.ExcelFile(input_file, engine='openpyxl')
            sheet_names = xl.sheet_names
            print(f"Found {len(sheet_names)} sheets: {', '.join(sheet_names)}")
            
            # Check if 'Orders' sheet exists
            if 'Orders' not in sheet_names:
                print("Error: The file must contain a sheet named 'Orders'")
                return None
                
            # Read the Orders sheet while preserving all original values exactly
            print("Reading sheet: Orders")
            
            # First, read the file with all values as strings to prevent any type inference
            df = pd.read_excel(input_file, sheet_name='Orders', engine='openpyxl', dtype=str, keep_default_na=False)
            print(f"Sheet 'Orders' loaded successfully using openpyxl engine")
            
            # Update 'Command' column from 'NEW' to 'MERGE' if it exists
            if 'Command' in df.columns:
                df['Command'] = df['Command'].replace('NEW', 'MERGE')
                print("Updated 'Command' column: Changed 'NEW' to 'MERGE'")
            
            # Process order groups if Top Row column exists
            if 'Top Row' in df.columns:
                # Find and process order groups
                order_groups = get_order_groups(df)
                if order_groups:
                    print(f"Processing {len(order_groups)} order groups...")
                    df = propagate_order_data(df, order_groups)
            
            # Ensure TRUE/FALSE values are in uppercase
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].apply(lambda x: x.upper() if isinstance(x, str) and x.upper() in ['TRUE', 'FALSE'] else x)
            
            # Check for empty 'Name' column to determine where to stop processing
            if 'Name' in df.columns:
                # Find first empty row in 'Name' column
                empty_name_mask = df['Name'].isna() | (df['Name'].astype(str).str.strip() == '')
                first_empty_idx = empty_name_mask.idxmax() if empty_name_mask.any() else len(df)
                
                if first_empty_idx < len(df):
                    print(f"Found empty 'Name' at row {first_empty_idx + 1}, truncating data...")
                    df = df.iloc[:first_empty_idx].copy()
                    print(f"Truncated to {len(df)} rows")
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
            'Billing: City',
            'Billing: Country Code',  # Priority over 'Billing: Country'
            'Billing: Country',       # Fallback if Country Code is missing
            'Shipping: First Name',
            'Shipping: Last Name', 
            'Shipping: Phone',
            'Shipping: Address 1',
            'Shipping: City',
            'Shipping: Country',
            'Shipping: Country Code'
        ]
        
        # Ensure shipping country columns exist in the dataframe
        for col in ['Shipping: Country', 'Shipping: Country Code']:
            if col not in df.columns:
                df[col] = ''
                print(f"Added missing column: {col}")
        
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
        
        # Process each column in the final dataframe
        print("\nProcessing final data...")
        for col in existing_columns:
            missing_before = df[col].isna().sum() + (df[col].astype(str).str.strip() == '').sum()
            if missing_before > 0:
                if col == 'Customer: Email':
                    df[col] = df[col].fillna('shopify@getnada.com').replace('', 'shopify@getnada.com')
                    df[col] = df[col].astype(str).str.strip()
                
                # Handle Phone fields - check between Customer, Billing, Shipping, and WooCommerce phone fields
                elif col in ['Billing: Phone', 'Shipping: Phone']:
                    # Define all possible phone columns to check, including WooCommerce specific ones
                    all_phone_cols = [                        
                        'Billing: Phone', 
                        'Shipping: Phone',
                        'Metafield: woo._billing_tel',
                        'Metafield: woo.billing_tel'
                    ]
                    
                    # Remove current column from the list of columns to check
                    other_phone_cols = [c for c in all_phone_cols if c != col and c in df.columns]
                    
                    # Create a mask for empty or NaN values in the current column
                    mask = (df[col].isna() | (df[col].astype(str).str.strip() == ''))
                    
                    # First, check if we have a Top Row with a phone number for this order
                    if 'Top Row' in df.columns and col in df.columns:
                        # Find all Top Rows with a phone number
                        top_rows_with_phone = df[(df['Top Row'].notna()) & 
                                              (df['Top Row'] != '') & 
                                              (df[col].notna()) & 
                                              (df[col] != '')]
                        
                        # For each Top Row with a phone number, update all rows with the same Name
                        for _, top_row in top_rows_with_phone.iterrows():
                            order_name = top_row['Name']
                            if pd.notna(order_name) and order_name != '':
                                # Update all rows with the same Name to use the Top Row's phone number
                                name_mask = (df['Name'] == order_name) & mask
                                if name_mask.any():
                                    df.loc[name_mask, col] = top_row[col]
                                    print(f"  Updated {name_mask.sum()} rows in order '{order_name}' with Top Row's {col}")
                                    # Update the mask for the next iteration
                                    mask = (df[col].isna() | (df[col].astype(str).str.strip() == ''))
                    
                    # Then check other phone columns in the same row
                    for other_col in other_phone_cols:
                        if other_col in df.columns and mask.any():
                            # Find rows where current phone is empty but other phone has value
                            other_phone_mask = mask & (~df[other_col].isna() & (df[other_col].astype(str).str.strip() != ''))
                            if other_phone_mask.any():
                                df.loc[other_phone_mask, col] = df.loc[other_phone_mask, other_col]
                                # Update the mask for the next iteration
                                mask = (df[col].isna() | (df[col].astype(str).str.strip() == ''))
                    
                    # Finally, if still empty after Top Row propagation and cross-field checks, set default phone
                    if mask.any():
                        df.loc[mask, col] = '+359-888888888'
                        print(f"  Filled {mask.sum()} missing {col} with default value")
                
                # Handle Name fields
                elif col in ['Billing: First Name', 'Shipping: First Name']:
                    other_name_col = 'Shipping: First Name' if col == 'Billing: First Name' else 'Billing: First Name'
                    if other_name_col in df.columns:
                        mask = (df[col].isna() | (df[col].astype(str).str.strip() == '')) & \
                               (~df[other_name_col].isna() & (df[other_name_col].astype(str).str.strip() != ''))
                        df.loc[mask, col] = df.loc[mask, other_name_col]
                    df[col] = df[col].fillna('Shopify').replace('', 'Shopify')
                
                # Handle Last Name fields
                elif col in ['Billing: Last Name', 'Shipping: Last Name']:
                    other_name_col = 'Shipping: Last Name' if col == 'Billing: Last Name' else 'Billing: Last Name'
                    if other_name_col in df.columns:
                        mask = (df[col].isna() | (df[col].astype(str).str.strip() == '')) & \
                               (~df[other_name_col].isna() & (df[other_name_col].astype(str).str.strip() != ''))
                        df.loc[mask, col] = df.loc[mask, other_name_col]
                    df[col] = df[col].fillna('Shopify').replace('', 'Shopify')
                
                # Handle Address 1 fields
                elif col in ['Billing: Address 1', 'Shipping: Address 1']:
                    other_addr_col = 'Shipping: Address 1' if col == 'Billing: Address 1' else 'Billing: Address 1'
                    if other_addr_col in df.columns:
                        mask = (df[col].isna() | (df[col].astype(str).str.strip() == '')) & \
                               (~df[other_addr_col].isna() & (df[other_addr_col].astype(str).str.strip() != ''))
                        df.loc[mask, col] = df.loc[mask, other_addr_col]
                    df[col] = df[col].fillna('Shopify').replace('', 'Shopify')
                
                # Handle City fields
                elif col in ['Billing: City', 'Shipping: City']:
                    other_city_col = 'Shipping: City' if col == 'Billing: City' else 'Billing: City'
                    if other_city_col in df.columns:
                        mask = (df[col].isna() | (df[col].astype(str).str.strip() == '')) & \
                               (~df[other_city_col].isna() & (df[other_city_col].astype(str).str.strip() != ''))
                        df.loc[mask, col] = df.loc[mask, other_city_col]
                    df[col] = df[col].fillna('Shopify').replace('', 'Shopify')
                
                # Handle Country fields
                elif col == 'Billing: Country':
                    df[col] = df[col].fillna('Bulgaria').replace('', 'Bulgaria')
                elif col == 'Billing: Country Code':
                    df[col] = df[col].fillna('BG').replace('', 'BG')
                elif col == 'Shipping: Country':
                    df[col] = df[col].fillna('Bulgaria').replace('', 'Bulgaria')
                elif col == 'Shipping: Country Code':
                    df[col] = df[col].fillna('BG').replace('', 'BG')
                
                missing_after = df[col].isna().sum() + (df[col].astype(str).str.strip() == '').sum()
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
        
        # Add 'judgeme_excluded' tag to all rows in the 'Tags' column
        if 'Tags' in df.columns:
            # If Tags column exists, append 'judgeme_excluded' to existing tags
            df['Tags'] = df['Tags'].fillna('').apply(
                lambda x: f"{x}, judge_me_excluded" if x else "judgeme_excluded"
            )
        else:
            # If Tags column doesn't exist, create it with 'judgeme_excluded'
            df['Tags'] = 'judgeme_excluded'
        
        print("\nAdded 'judgeme_excluded' tag to all rows in the 'Tags' column")
        
        # Read all sheets from the original file
        xl = pd.ExcelFile(input_file, engine='openpyxl')
        sheet_data = {}
        for sheet_name in xl.sheet_names:
            if sheet_name == 'Orders':
                # Use our processed dataframe for the Orders sheet
                sheet_data[sheet_name] = df
            else:
                # Read other sheets as-is
                sheet_data[sheet_name] = pd.read_excel(xl, sheet_name=sheet_name)
        
        # Save all sheets to the output file
        try:
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                for sheet_name, sheet_df in sheet_data.items():
                    # Preserve all values exactly as they are
                    # No type conversion or value modification will be done
                    sheet_df.to_excel(writer, index=False, sheet_name=sheet_name)
            print(f"File saved successfully with {len(sheet_data)} sheets")
            
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
        
        # Check for missing shipping information in the summary
        missing_shipping_country = 0
        missing_shipping_code = 0
        
        if 'Shipping: Country' in df.columns:
            missing_shipping_country = (df['Shipping: Country'].isna() | (df['Shipping: Country'].astype(str).str.strip() == '')).sum()
        if 'Shipping: Country Code' in df.columns:
            missing_shipping_code = (df['Shipping: Country Code'].isna() | (df['Shipping: Country Code'].astype(str).str.strip() == '')).sum()
        
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
        
        # Add shipping information to the report
        if 'Shipping: Country' in df.columns or 'Shipping: Country Code' in df.columns:
            print("\nShipping Information:")
            if 'Shipping: Country' in df.columns:
                print(f"  - Missing Shipping Country: {missing_shipping_country} rows")
            if 'Shipping: Country Code' in df.columns:
                print(f"  - Missing Shipping Country Code: {missing_shipping_code} rows")
        
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
