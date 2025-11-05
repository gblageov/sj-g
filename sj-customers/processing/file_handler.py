"""
File Handler Module
Handles reading and writing Excel files with proper data preservation.
"""

import pandas as pd
import os
from datetime import datetime


def read_excel_file(input_file: str) -> tuple[pd.DataFrame, list]:
    """
    Read Excel file and return the Orders sheet with all other sheet names.
    
    Args:
        input_file: Path to input Excel file
        
    Returns:
        Tuple of (Orders DataFrame, list of all sheet names)
    """
    print(f"Reading file: {input_file}")
    
    try:
        xl = pd.ExcelFile(input_file, engine='openpyxl')
        sheet_names = xl.sheet_names
        print(f"Found {len(sheet_names)} sheets: {', '.join(sheet_names)}")
        
        # Check if 'Orders' sheet exists
        if 'Orders' not in sheet_names:
            print("Error: The file must contain a sheet named 'Orders'")
            return None, None
            
        # Read the Orders sheet while preserving all original values exactly
        print("Reading sheet: Orders")
        
        # First, read the file with all values as strings to prevent any type inference
        df = pd.read_excel(input_file, sheet_name='Orders', engine='openpyxl', dtype=str, keep_default_na=False)
        print(f"Sheet 'Orders' loaded successfully using openpyxl engine")
        
        return df, sheet_names
        
    except Exception as e1:
        print(f"Failed to read with openpyxl: {e1}")
        try:
            df = pd.read_excel(input_file, engine='xlrd')
            print(f"File read successfully using xlrd engine")
            xl = pd.ExcelFile(input_file, engine='xlrd')
            return df, xl.sheet_names
        except Exception as e2:
            print(f"Failed to read with xlrd: {e2}")
            try:
                df = pd.read_excel(input_file)  # Default engine
                print(f"File read successfully using default engine")
                xl = pd.ExcelFile(input_file)
                return df, xl.sheet_names
            except Exception as e3:
                print(f"Failed to read with default engine: {e3}")
                raise Exception(f"Unable to read Excel file: {e3}")


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize DataFrame values (TRUE/FALSE, Command column).
    
    Args:
        df: Input DataFrame
        
    Returns:
        Normalized DataFrame
    """
    # Update 'Command' column from 'NEW' to 'MERGE' if it exists
    if 'Command' in df.columns:
        df['Command'] = df['Command'].replace('NEW', 'MERGE')
        print("Updated 'Command' column: Changed 'NEW' to 'MERGE'")
    
    # Ensure TRUE/FALSE values are in uppercase
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: x.upper() if isinstance(x, str) and x.upper() in ['TRUE', 'FALSE'] else x)
    
    return df


def truncate_by_empty_name(df: pd.DataFrame) -> pd.DataFrame:
    """
    Truncate data at first empty row in 'Name' column.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Truncated DataFrame
    """
    if 'Name' in df.columns:
        # Find first empty row in 'Name' column
        empty_name_mask = df['Name'].isna() | (df['Name'].astype(str).str.strip() == '')
        first_empty_idx = empty_name_mask.idxmax() if empty_name_mask.any() else len(df)
        
        if first_empty_idx < len(df):
            print(f"Found empty 'Name' at row {first_empty_idx + 1}, truncating data...")
            df = df.iloc[:first_empty_idx].copy()
            print(f"Truncated to {len(df)} rows")
    
    return df


def save_excel_file(df: pd.DataFrame, input_file: str, output_file: str = None) -> str:
    """
    Save processed DataFrame to Excel file along with all other sheets.
    
    Args:
        df: Processed Orders DataFrame
        input_file: Path to input Excel file
        output_file: Path to output Excel file (optional)
        
    Returns:
        Path to output file if successful, None otherwise
    """
    # Generate output filename if not provided
    if output_file is None:
        base_name = os.path.splitext(input_file)[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"{base_name}_{timestamp}.xlsx"
    
    # Save the fixed file
    print(f"\nSaving fixed file to: {output_file}")
    print("Please wait... Processing and saving the file (this may take a moment for large files)")
    
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
    
    return output_file


def print_column_statistics(df: pd.DataFrame, existing_columns: list, title: str):
    """
    Print statistics for columns before or after processing.
    
    Args:
        df: DataFrame to analyze
        existing_columns: List of columns to check
        title: Title for the statistics section
    """
    print(f"\n" + "=" * 50)
    print(title)
    print("=" * 50)
    
    for col in existing_columns:
        filled_count = df[col].notna().sum() - (df[col] == '').sum()
        missing_count = df[col].isna().sum() + (df[col] == '').sum()
        print(f"{col} - {filled_count} filled fields, {missing_count} missing fields")


def print_summary_report(input_file: str, output_file: str, df: pd.DataFrame, 
                        existing_columns: list, missing_columns: list, 
                        rows_with_missing: list, fixed_count: int, remaining_missing: int):
    """
    Print comprehensive summary report.
    
    Args:
        input_file: Path to input file
        output_file: Path to output file
        df: Processed DataFrame
        existing_columns: List of existing required columns
        missing_columns: List of missing required columns
        rows_with_missing: List of rows with missing data
        fixed_count: Total number of fixed values
        remaining_missing: Number of remaining missing values
    """
    # Check for missing shipping information
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
