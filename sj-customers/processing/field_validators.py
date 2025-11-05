"""
Field Validation Module
Handles validation and filling of required fields for Shopify import.
"""

import pandas as pd
from typing import List


# Constants
DEFAULT_EMAIL = 'shopify@getnada.com'
DEFAULT_PHONE = '+1234567890'
DEFAULT_TEXT_VALUE = 'Shopify'
DEFAULT_COUNTRY = 'Bulgaria'
DEFAULT_COUNTRY_CODE = 'BG'

# Required fields for Shopify import
REQUIRED_FIELDS = [
    'Customer: Email',
    'Customer: Phone',
    'Billing: First Name', 
    'Billing: Last Name',
    'Billing: Phone',
    'Billing: Address 1',
    'Billing: City',
    'Billing: Country Code',
    'Billing: Country',
    'Shipping: First Name',
    'Shipping: Last Name', 
    'Shipping: Phone',
    'Shipping: Address 1',
    'Shipping: City',
    'Shipping: Country',
    'Shipping: Country Code'
]


def get_existing_required_columns(df: pd.DataFrame) -> tuple[List[str], List[str]]:
    """
    Check which required columns exist in the dataframe.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Tuple of (existing_columns, missing_columns)
    """
    existing_columns = [col for col in REQUIRED_FIELDS if col in df.columns]
    missing_columns = [col for col in REQUIRED_FIELDS if col not in df.columns]
    
    return existing_columns, missing_columns


def validate_email_field(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Validate and fill email field."""
    df[col] = df[col].fillna(DEFAULT_EMAIL).replace('', DEFAULT_EMAIL)
    df[col] = df[col].astype(str).str.strip()
    return df


def validate_phone_fields(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    Validate and fill phone fields with cross-column checking.
    Checks Customer Phone, Billing Phone, Shipping Phone, and WooCommerce metafields.
    """
    # Define all possible phone columns to check, including WooCommerce specific ones
    all_phone_cols = [
        'Customer: Phone', 
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
        df.loc[mask, col] = DEFAULT_PHONE
        print(f"  Filled {mask.sum()} missing {col} with default value")
    
    return df


def validate_name_fields(df: pd.DataFrame, col: str, other_col: str) -> pd.DataFrame:
    """Validate and fill name fields with cross-column checking."""
    if other_col in df.columns:
        mask = (df[col].isna() | (df[col].astype(str).str.strip() == '')) & \
               (~df[other_col].isna() & (df[other_col].astype(str).str.strip() != ''))
        df.loc[mask, col] = df.loc[mask, other_col]
    df[col] = df[col].fillna(DEFAULT_TEXT_VALUE).replace('', DEFAULT_TEXT_VALUE)
    return df


def validate_address_fields(df: pd.DataFrame, col: str, other_col: str) -> pd.DataFrame:
    """Validate and fill address fields with cross-column checking."""
    if other_col in df.columns:
        mask = (df[col].isna() | (df[col].astype(str).str.strip() == '')) & \
               (~df[other_col].isna() & (df[other_col].astype(str).str.strip() != ''))
        df.loc[mask, col] = df.loc[mask, other_col]
    df[col] = df[col].fillna(DEFAULT_TEXT_VALUE).replace('', DEFAULT_TEXT_VALUE)
    return df


def validate_city_fields(df: pd.DataFrame, col: str, other_col: str) -> pd.DataFrame:
    """Validate and fill city fields with cross-column checking."""
    if other_col in df.columns:
        mask = (df[col].isna() | (df[col].astype(str).str.strip() == '')) & \
               (~df[other_col].isna() & (df[other_col].astype(str).str.strip() != ''))
        df.loc[mask, col] = df.loc[mask, other_col]
    df[col] = df[col].fillna(DEFAULT_TEXT_VALUE).replace('', DEFAULT_TEXT_VALUE)
    return df


def validate_country_fields(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Validate and fill country fields."""
    if 'Country Code' in col:
        df[col] = df[col].fillna(DEFAULT_COUNTRY_CODE).replace('', DEFAULT_COUNTRY_CODE)
    else:
        df[col] = df[col].fillna(DEFAULT_COUNTRY).replace('', DEFAULT_COUNTRY)
    return df


def process_all_fields(df: pd.DataFrame, existing_columns: List[str]) -> tuple[pd.DataFrame, int]:
    """
    Process all required fields and fill missing values.
    
    Args:
        df: Input DataFrame
        existing_columns: List of existing required columns
        
    Returns:
        Tuple of (modified DataFrame, total fixed count)
    """
    fixed_count = 0
    
    print("\nProcessing final data...")
    for col in existing_columns:
        missing_before = df[col].isna().sum() + (df[col].astype(str).str.strip() == '').sum()
        if missing_before > 0:
            if col == 'Customer: Email':
                df = validate_email_field(df, col)
            
            # Handle Phone fields
            elif col in ['Customer: Phone', 'Billing: Phone', 'Shipping: Phone']:
                df = validate_phone_fields(df, col)
            
            # Handle Name fields
            elif col in ['Billing: First Name', 'Shipping: First Name']:
                other_name_col = 'Shipping: First Name' if col == 'Billing: First Name' else 'Billing: First Name'
                df = validate_name_fields(df, col, other_name_col)
            
            # Handle Last Name fields
            elif col in ['Billing: Last Name', 'Shipping: Last Name']:
                other_name_col = 'Shipping: Last Name' if col == 'Billing: Last Name' else 'Billing: Last Name'
                df = validate_name_fields(df, col, other_name_col)
            
            # Handle Address 1 fields
            elif col in ['Billing: Address 1', 'Shipping: Address 1']:
                other_addr_col = 'Shipping: Address 1' if col == 'Billing: Address 1' else 'Billing: Address 1'
                df = validate_address_fields(df, col, other_addr_col)
            
            # Handle City fields
            elif col in ['Billing: City', 'Shipping: City']:
                other_city_col = 'Shipping: City' if col == 'Billing: City' else 'Billing: City'
                df = validate_city_fields(df, col, other_city_col)
            
            # Handle Country fields
            elif col in ['Billing: Country', 'Billing: Country Code', 'Shipping: Country', 'Shipping: Country Code']:
                df = validate_country_fields(df, col)
            
            missing_after = df[col].isna().sum() + (df[col].astype(str).str.strip() == '').sum()
            print(f"  Fixed {col}: {missing_before} missing values -> {missing_after} missing")
            fixed_count += missing_before
    
    return df, fixed_count


def add_judgeme_tag(df: pd.DataFrame) -> pd.DataFrame:
    """Add 'judgeme_excluded' tag to all rows."""
    if 'Tags' in df.columns:
        # If Tags column exists, append 'judgeme_excluded' to existing tags
        df['Tags'] = df['Tags'].fillna('').apply(
            lambda x: f"{x}, judge_me_excluded" if x else "judgeme_excluded"
        )
    else:
        # If Tags column doesn't exist, create it with 'judgeme_excluded'
        df['Tags'] = 'judgeme_excluded'
    
    print("\nAdded 'judgeme_excluded' tag to all rows in the 'Tags' column")
    return df
