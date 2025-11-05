"""
Order Processing Module
Handles order grouping and data propagation from Top Row to order items.
"""

import pandas as pd
from typing import Dict, Any


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
    standard_phone_fields = ['Customer: Phone', 'Billing: Phone', 'Shipping: Phone']
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
