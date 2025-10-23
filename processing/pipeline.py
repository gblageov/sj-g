import sys
import re
import os
from datetime import datetime
from typing import Optional
import pandas as pd

from .constants import PRODUCT_TYPES, TARGET_METAFIELD_COLUMN
from . import io as io_mod
from .type_detection import populate_type_column
from .mapping import build_sku_to_handle, build_woo_id_to_handle
from .parsing import extract_woobt_dict
from .report import print_summary_report


def process_woocommerce_to_shopify(file_path: str, output_file: str = None) -> Optional[str]:
    """
    Orchestrates the processing pipeline equivalent to the original implementation.
    
    Args:
        file_path: Path to the input Excel file
        output_file: Optional path for the output Excel file. If not provided, will use
                   the input filename with '_output' suffix.
                   
    Returns:
        str: Path to the output file on success, None on error
    """
    # Read the input file
    df = io_mod.read_products_df(file_path)
    if df is None:
        return None
        
    # Remove columns containing 'Metafield: woo.xts-blocks' in their names
    print("\nПремахване на колони, съдържащи 'Metafield: woo.xts-blocks'...")
    df, removed_count = io_mod.remove_xts_blocks_columns(df)
    if removed_count > 0:
        print(f"-> Премахнати са {removed_count} колони, съдържащи 'Metafield: woo.xts-blocks' в името.")
    else:
        print("-> Не са намерени колони за премахване, съдържащи 'Metafield: woo.xts-blocks' в името.")

    # Populate 'Type'
    print("\nЗапочва попълване на колона 'Type' (търсене на най-пълно съвпадение)...")
    types_added_count = populate_type_column(df, PRODUCT_TYPES)
    print(f"-> Попълването приключи. Добавени са {types_added_count} типа в колона 'Type'.")

    # Diagnostic: Report products without Type
    rows_without_type = []
    for idx, row in df.iterrows():
        if not row.get('Type') or pd.isna(row.get('Type')):
            title = row.get('Title', '')
            rows_without_type.append((idx + 2, title))  # +2 for Excel row (0-indexed idx + header row)

    if rows_without_type:
        print("\nДИАГНОСТИКА: ПРОДУКТИ БЕЗ ТИП")
        print("-" * 50)
        print(f"Намерени са {len(rows_without_type)} продукти без попълнен тип:")
        for row_num, title in rows_without_type:
            print(f"  Ред {row_num}: {title}")
    else:
        print("\n-> Всички продукти имат попълнен тип.")

    # Build lookup maps for Combined handle
    print("\nЗапочва създаване на речници за търсене за 'Combined handle'...")
    sku_to_handle = build_sku_to_handle(df)
    woo_id_to_handle = build_woo_id_to_handle(df)
    print(f"-> Създаден е речник с {len(sku_to_handle)} уникални SKU-та.")
    print(f"-> Създаден е речник с {len(woo_id_to_handle)} уникални Woo ID-та.")

    rows_with_woobt_data = df['Metafield: woo.woobt_ids'].notna().sum()
    print(f"--> Намерени са {rows_with_woobt_data} реда с данни в 'Metafield: woo.woobt_ids', които ще бъдат обработени.")

    updated_count = 0
    rows_with_data_count = 0
    json_parse_errors = []
    unmatched_products = []

    print("\nЗапочва обработка на 'Combined handle'...")
    for idx, row in df.iterrows():
        woobt_ids = row['Metafield: woo.woobt_ids']
        if pd.isna(woobt_ids) or str(woobt_ids).strip() == '':
            continue

        rows_with_data_count += 1
        # Show progress every 100 rows to improve performance
        if rows_with_data_count % 100 == 0 or rows_with_data_count == rows_with_woobt_data:
            print(f"Обработване на ред {rows_with_data_count} от {rows_with_woobt_data}...", end='\r')
            sys.stdout.flush()

        excel_row_num = idx + 2
        try:
            data_dict, parse_error = extract_woobt_dict(woobt_ids)
            if parse_error:
                json_parse_errors.append(f"Ред {excel_row_num}: {parse_error}")
                continue
            if not isinstance(data_dict, dict) or not data_dict:
                continue

            products_data = []
            for k, v in data_dict.items():
                if isinstance(v, dict):
                    sku = str(v.get('sku', '')).strip()
                    pid = str(v.get('id', '')).strip()
                    products_data.append({'sku': sku, 'id': pid})

            if not products_data:
                continue

            matching_handles = []
            row_unmatched_products = []
            for product in products_data:
                sku, product_id = product['sku'], product['id']
                found_handle = sku_to_handle.get(sku) or woo_id_to_handle.get(product_id)
                if found_handle:
                    matching_handles.append(found_handle)
                else:
                    row_unmatched_products.append(f"SKU: '{sku}'/ID: '{product_id}'")

            if row_unmatched_products:
                unmatched_products.append(
                    f"Ред {excel_row_num}: Не са намерени съвпадения за -> {', '.join(row_unmatched_products)}"
                )

            if matching_handles:
                df.at[idx, TARGET_METAFIELD_COLUMN] = ','.join(list(set(matching_handles)))
                updated_count += 1

        except Exception as e:
            print(f"Критична грешка при обработка на ред {excel_row_num}: {e}")
            continue

    # Determine output file path if not provided
    if output_file is None:
        base, ext = os.path.splitext(file_path)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"{base}_output_{timestamp}{ext}"

    # Ensure the output directory exists
    output_dir = os.path.dirname(os.path.abspath(output_file))
    os.makedirs(output_dir, exist_ok=True)

    print("\n" + "="*80)
    print("ЗАПАЗВАНЕ НА ФАЙЛА СЪС ЗАПАЗВАНЕ НА ВСИЧКИ ЛИСТОВЕ (ОСВЕН 'prodct')")
    print("="*80)
    
    # First write the main Products sheet
    print("\nЗапис на обработените данни в крайния файл...")
    
    # Create a temporary file for the main data
    temp_products = os.path.join(output_dir, f"temp_products_{os.urandom(4).hex()}.xlsx")
    df.to_excel(temp_products, index=False, sheet_name='Products', engine='xlsxwriter')
    
    try:
        # Now create the final output file with all sheets
        with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
            # First write the processed Products sheet with exact name 'Products'
            df.to_excel(writer, sheet_name='Products', index=False)
            
            # Then copy all other sheets from the original file except 'prodct' (case insensitive)
            # and any sheet that might be a duplicate of 'Products' (case insensitive)
            xls = pd.ExcelFile(file_path, engine='openpyxl')
            sheets_copied = 0
            
            for sheet_name in xls.sheet_names:
                # Skip 'prodct' (case insensitive) and 'Products' (case insensitive)
                lower_sheet_name = sheet_name.lower()
                if lower_sheet_name != 'prodct' and lower_sheet_name != 'products':
                    print(f"  - Добавяне на лист: {sheet_name}")
                    try:
                        sheet_df = pd.read_excel(xls, sheet_name=sheet_name)
                        sheet_df.to_excel(writer, sheet_name=sheet_name, index=False)
                        sheets_copied += 1
                    except Exception as e:
                        print(f"    Грешка при копиране на лист {sheet_name}: {str(e)}")
            
            # Ensure we don't have any duplicate 'Products' sheets (case insensitive)
            if 'Products' not in writer.sheets:
                # This should never happen as we write it first, but just in case
                df.to_excel(writer, sheet_name='Products', index=False)
            
            print(f"\nУспешно са копирани {sheets_copied} допълнителни листа от оригиналния файл.")
        
        print(f"\nФайлът е запазен успешно като: {output_file}")
        
    finally:
        # Clean up the temporary file
        if os.path.exists(temp_products):
            os.remove(temp_products)
    
    # Print summary report
    print_summary_report(
        total_products=len(df),
        rows_with_data=rows_with_data_count,
        updated_count=updated_count,
        json_parse_errors=json_parse_errors,
        unmatched_products=unmatched_products,
        output_file=output_file
    )
    
    print("\n" + "="*80)
    print("ОБРАБОТКАТА ПРИКЛЮЧИ УСПЕШНО!")
    print(f"Резултатният файл е запазен като: {output_file}")
    print("="*80)
    
    return output_file
