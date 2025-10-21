import os
import sys
import pandas as pd
from .constants import TARGET_METAFIELD_COLUMN


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


def write_products_df(df: pd.DataFrame, output_path: str):
    """
    Writes the DataFrame to the given Excel path with the 'Products' sheet.
    Prints brief diagnostics to match current behavior.
    """
    print("\nОбработката на редовете приключи. Започва запис на новия Excel файл...")
    print("Тази стъпка може да отнеме известно време, моля изчакайте...")
    df.to_excel(output_path, index=False, sheet_name='Products', engine='xlsxwriter')
    print(f"\nОбновеният файл е запазен като: {output_path}")
