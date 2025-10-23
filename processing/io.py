import os
import sys
import pandas as pd
from typing import Tuple
from .constants import TARGET_METAFIELD_COLUMN, COLUMN_RENAMES


def read_products_df(file_path: str):
    """
    Reads the Products sheet from the given Excel file and ensures required columns exist.
    Returns a pandas DataFrame or None on error. Prints diagnostics matching current behavior.
    """
    if not os.path.exists(file_path):
        print(f"ГРЕШКА: Файлът не е намерен на адрес: {file_path}")
        return None

    try:
        df = pd.read_excel(file_path, sheet_name='Products', engine='openpyxl')
        print(f"Файлът '{file_path}' е прочетен успешно. Общо редове: {len(df)}")
    except Exception as e:
        print(f"ГРЕШКА при четене на Excel файла: {e}")
        return None

    # Apply column renames (Bulgarian -> English) where applicable
    # Only rename when the old name exists and the target name does not already exist to avoid collisions
    renames_to_apply = {}
    for old, new in COLUMN_RENAMES.items():
        if old in df.columns and new not in df.columns:
            renames_to_apply[old] = new
    if renames_to_apply:
        df.rename(columns=renames_to_apply, inplace=True)

    # Normalize engraving enable column values: 'yes' -> 'True', 'no' -> 'False' (as text)
    # Handle possible column naming variants (with/without 'Metafield: ' prefix)
    engrave_candidates = [
        c for c in df.columns
        if c.strip() == 'Metafield: woo.bgfd_enable_product_engraving' or c.strip().endswith('Metafield: woo.bgfd_enable_product_engraving')
    ]
    for engrave_col in engrave_candidates:
        def _normalize_engrave(v):
            s = '' if pd.isna(v) else str(v).strip().lower()
            if s == 'yes':
                return 'True'
            if s == 'no':
                return 'False'
            return v
        df[engrave_col] = df[engrave_col].apply(_normalize_engrave).astype(object)

    # Ensure Combined handle column exists at correct position (before woo.woobt_ids)
    if TARGET_METAFIELD_COLUMN not in df.columns:
        print(f"Забележка: Целевата колона '{TARGET_METAFIELD_COLUMN}' липсва. Тя ще бъде създадена автоматично.")
        try:
            reference_col_index = df.columns.get_loc('Metafield: woo.woobt_ids')
            df.insert(loc=reference_col_index, column=TARGET_METAFIELD_COLUMN, value='')
            print(f"-> Колоната '{TARGET_METAFIELD_COLUMN}' е успешно създадена.")
        except KeyError:
            print(f"ГРЕШКА: Референтната колона 'Metafield: woo.woobt_ids' не е намерена.")
            return None

    # Ensure Type column exists right after Title
    if 'Type' not in df.columns:
        print(f"Забележка: Целевата колона 'Type' липсва. Тя ще бъде създадена автоматично.")
        try:
            title_col_index = df.columns.get_loc('Title')
            df.insert(loc=title_col_index + 1, column='Type', value='')
            print(f"-> Колоната 'Type' е успешно създадена след 'Title'.")
        except KeyError:
            print(f"ГРЕШКА: Задължителната колона 'Title' не е намерена, за да се добави колона 'Type'.")
            return None
    # Ensure dtype for 'Type' is object to avoid FutureWarning when setting strings
    df['Type'] = df['Type'].astype(object)

    # Required columns check (must mirror current implementation)
    required_columns = [
        'Metafield: woo.woobt_ids', 'Variant SKU', 'Handle',
        TARGET_METAFIELD_COLUMN, 'Metafield: woo.id', 'Variant Metafield: woo.id', 'Title'
    ]
    for col in required_columns:
        if col not in df.columns:
            print(f"ГРЕШКА: Липсва задължителна колона '{col}' във файла.")
            return None

    # Ensure dtype for TARGET_METAFIELD_COLUMN is object
    df[TARGET_METAFIELD_COLUMN] = df[TARGET_METAFIELD_COLUMN].astype(object)

    return df


def remove_xts_blocks_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    """
    Removes all columns containing 'Metafield: woo.xts-blocks' in their names.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Tuple containing:
        - DataFrame with columns removed
        - Number of columns that were removed
    """
    # Find all columns containing 'Metafield: woo.xts-blocks' in their names
    columns_to_remove = [col for col in df.columns if 'Metafield: woo.xts-blocks' in str(col)]
    
    if not columns_to_remove:
        return df, 0
        
    # Remove the columns
    df = df.drop(columns=columns_to_remove)
    
    return df, len(columns_to_remove)


def copy_other_sheets(input_path: str, output_path: str) -> bool:
    """
    Copies all sheets except 'prodct' and 'Products' from input Excel file to a new Excel file.
    Adds logging and verification of the copied data.
    
    Args:
        input_path: Path to the input Excel file
        output_path: Path to save the output Excel file
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        print(f"\nЗапочва копиране на всички листове освен 'prodct' и 'Products' от {input_path}...")
        
        # Read all sheets from the input file
        xls = pd.ExcelFile(input_path, engine='openpyxl')
        sheet_names = [sheet for sheet in xls.sheet_names 
                      if sheet.lower() not in ['prodct', 'products']]
        
        if not sheet_names:
            print("ВНИМАНИЕ: Не са намерени листове за копиране (освен 'prodct' и 'Products').")
            return False
            
        print(f"Намерени са {len(sheet_names)} листа за копиране: {', '.join(sheet_names)}")
        
        # Create a new Excel writer
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            # Process each sheet
            for sheet_name in sheet_names:
                print(f"\nОбработка на лист '{sheet_name}'...")
                
                # Read the sheet
                df = pd.read_excel(xls, sheet_name=sheet_name)
                row_count = len(df)
                print(f"-> Листът '{sheet_name}' съдържа {row_count} реда и {len(df.columns)} колони.")
                
                # Write to the new file
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"-> Листът '{sheet_name}' е копиран успешно.")
                
                # Verify random rows
                if row_count > 0:
                    print("  Проверка на произволни редове:")
                    sample_size = min(5, row_count)
                    random_rows = df.sample(n=sample_size, random_state=42)
                    
                    for idx, row in random_rows.iterrows():
                        # Convert row to string, handle NaN values
                        row_str = ', '.join([f"{k}: {v if pd.notna(v) else 'NaN'}" 
                                          for k, v in row.items()])
                        print(f"  - Ред {idx+2}: {row_str[:100]}...")
        
        print(f"\nВсички листове са копирани успешно в: {output_path}")
        return True
        
    except Exception as e:
        print(f"ГРЕШКА при копиране на листовете: {str(e)}")
        return False


def write_products_df(df: pd.DataFrame, output_path: str, input_path: str = None):
    """
    Writes the DataFrame to the given Excel path with the 'Products' sheet.
    If input_path is provided, copies all other sheets from the input file.
    
    Args:
        df: DataFrame to write
        output_path: Path to save the output Excel file
        input_path: Optional path to the input Excel file to copy other sheets from
    """
    print("\nОбработката на редовете приключи. Започва запис на новия Excel файл...")
    print("Тази стъпка може да отнеме известно време, моля изчакайте...")
    
    # If input path is provided, copy all sheets except 'prodct' first
    if input_path and os.path.exists(input_path):
        # Create a temporary file for the main data
        temp_path = output_path.replace('.xlsx', '_temp.xlsx')
        df.to_excel(temp_path, index=False, sheet_name='Products', engine='xlsxwriter')
        
        # Now copy all sheets from temp and input files to final output
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            # Copy Products sheet first
            df.to_excel(writer, sheet_name='Products', index=False)
            
            # Copy all other sheets from input file except 'prodct'
            xls = pd.ExcelFile(input_path, engine='openpyxl')
            sheets_copied = 0
            
            for sheet_name in xls.sheet_names:
                if sheet_name.lower() != 'prodct' and sheet_name != 'Products':
                    sheet_df = pd.read_excel(xls, sheet_name=sheet_name)
                    sheet_df.to_excel(writer, sheet_name=sheet_name, index=False)
                    sheets_copied += 1
            
            print(f"\nУспешно са копирани {sheets_copied} допълнителни листа от оригиналния файл.")
            
    else:
        # If no input path, just write the Products sheet
        df.to_excel(output_path, index=False, sheet_name='Products', engine='xlsxwriter')
    
    print(f"\nОбновеният файл е запазен като: {output_path}")
    
    # If we created a temp file, clean it up
    if 'temp_path' in locals() and os.path.exists(temp_path):
        os.remove(temp_path)
