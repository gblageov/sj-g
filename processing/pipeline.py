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
from collections import defaultdict
import json
import os
from pathlib import Path


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

    # Update Vendor column from woocommerce-product-brand-export.xlsx if it exists
    print("\nПроверка за файл с марки на продуктите...")
    brand_file = Path("data/woocommerce-product-brand-export.xlsx")
    if brand_file.exists():
        print(f"-> Намерен е файл с марки на продуктите: {brand_file}")
        print(f"-> Брой редове преди актуализация: {len(df)}")
        print(f"-> Колони в DataFrame: {list(df.columns)[:20]}...")  # Print first 20 columns
        
        # Debug: Check for WooCommerce ID column
        woo_id_columns = [col for col in df.columns if 'woo.id' in col]
        print(f"-> Намерени колони с WooCommerce ID: {woo_id_columns}")
        
        updated_count = update_vendor_from_brand_export(df, str(brand_file))
        
        # Debug: Check Vendor column after update
        if 'Vendor' in df.columns:
            non_empty_vendors = df[df['Vendor'].notna() & (df['Vendor'] != '')].shape[0]
            print(f"-> След актуализация: {non_empty_vendors} продукта с попълнена марка (Vendor)")
        
        print(f"-> Актуализирани са общо {updated_count} продукта с информация за марка (Vendor).")
    else:
        print(f"-> Файл с марки на продуктите не е намерен на пътя: {brand_file.absolute()}")
        print("-> Създайте папка 'data' и поставете файла 'woocommerce-product-brand-export.xlsx' в нея.")
    
    # Populate 'Type' column
    print("\nЗапочва попълване на колона 'Type' (търсене на най-пълно съвпадение)...")
    types_added_count = populate_type_column(df, PRODUCT_TYPES)
    print(f"-> Попълването приключи. Добавени са {types_added_count} типа в колона 'Type'.")
    
    # Update types based on new-types.json mapping
    print("\nЗапочва актуализиране на типовете според new-types.json...")
    updated_types_count = update_product_types(df)
    if updated_types_count > 0:
        print(f"-> Актуализирани са {updated_types_count} типа според new-types.json")
    else:
        print("-> Не са намерени типове за актуализиране според new-types.json")

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
    updates_map = {}  # collect row updates and apply in bulk for speed

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
                updates_map[idx] = ','.join(list(set(matching_handles)))
                updated_count += 1

        except Exception as e:
            print(f"Критична грешка при обработка на ред {excel_row_num}: {e}")
            continue

    # Apply all updates in bulk for performance
    if updates_map:
        df.loc[list(updates_map.keys()), TARGET_METAFIELD_COLUMN] = list(updates_map.values())

    # Determine output file path if not provided
    if output_file is None:
        base, ext = os.path.splitext(file_path)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"{base}_output_{timestamp}{ext}"

    # Ensure the output directory exists
    output_dir = os.path.dirname(os.path.abspath(output_file))
    os.makedirs(output_dir, exist_ok=True)

    print("\n" + "="*80)
    print("ЗАПАЗВАНЕ НА ФАЙЛА СЪС ЗАПАЗВАНЕ НА ВСИЧКИ ЛИСТОВЕ (ОСВЕН 'Products')")
    print("="*80)
    
    print("\nЗапис на обработените данни в крайния файл...")
    
    # Create the final output file with all sheets in one go
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
        
        print(f"\nУспешно са копирани {sheets_copied} допълнителни листа от оригиналния файл.")
    
    print(f"\nФайлът е запазен успешно като: {output_file}")
    
    # Print summary report
    print_summary_report(
        types_added_count=types_added_count,
        rows_with_data_count=rows_with_data_count,
        updated_count=updated_count + updated_types_count,  # Include type updates in the total
        json_parse_errors=json_parse_errors,
        unmatched_products=unmatched_products,
        output_path=output_file
    )
    
    print("\n" + "="*80)
    print("ОБРАБОТКАТА ПРИКЛЮЧИ УСПЕШНО!")
    print(f"Резултатният файл е запазен като: {output_file}")
    
    # Generate product types report
    generate_product_types_report(df, output_dir)
    
    print("="*80)
    
    return output_file


def generate_product_types_report(df: pd.DataFrame, output_dir: str) -> str:
    """
    Generate a report of product types and their counts.
    
    Args:
        df: DataFrame containing the products data
        output_dir: Directory to save the report
        
    Returns:
        Path to the generated report file
    """
    print("\nГенериране на справка за продуктовите типове...")
    
    # Count occurrences of each product type
    type_counts = defaultdict(int)
    for product_type in df['Type'].dropna():
        if isinstance(product_type, str):  # Ensure it's a string
            type_counts[product_type.strip()] += 1
    
    if not type_counts:
        print("Не са намерени продуктови типове за анализ.")
        return ""
    
    # Create a DataFrame with the results
    report_data = []
    for p_type, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True):
        report_data.append({
            'Тип продукт': p_type,
            'Брой продукти': count,
            'Процент': f"{(count / len(df) * 100):.2f}%"
        })
    
    report_df = pd.DataFrame(report_data)
    
    # Generate output filename with timestamp
    timestamp = datetime.now().strftime("%m-%d-%H%M")
    report_filename = f"sj-types-{timestamp}.xlsx"
    report_path = os.path.join(output_dir, report_filename)
    
    # Save to Excel
    report_df.to_excel(report_path, index=False, engine='xlsxwriter')
    
    # Add some formatting to the Excel file
    with pd.ExcelWriter(report_path, engine='xlsxwriter') as writer:
        report_df.to_excel(writer, index=False, sheet_name='Product Types')
        
        # Get the xlsxwriter workbook and worksheet objects
        workbook = writer.book
        worksheet = writer.sheets['Product Types']
        
        # Add a header format
        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'bg_color': '#4472C4',
            'font_color': 'white',
            'border': 1
        })
        
        # Write the column headers with the defined format
        for col_num, value in enumerate(report_df.columns.values):
            worksheet.write(0, col_num, value, header_format)
        
        # Set column widths
        worksheet.set_column('A:A', 40)  # Product Type
        worksheet.set_column('B:C', 20)  # Count and Percentage
        
        # Add a table with autofilter
        (max_row, max_col) = report_df.shape
        column_settings = [{'header': column} for column in report_df.columns]
        worksheet.add_table(0, 0, max_row, max_col - 1, {
            'columns': column_settings,
            'style': 'Table Style Medium 2',
            'name': 'ProductTypesTable',
            'autofilter': True
        })
    
    print(f"Справката за продуктовите типове е запазена като: {report_path}")
    
    # Print summary
    total_products = sum(type_counts.values())
    unique_types = len(type_counts)
    print(f"\nОбщ брой продукти: {total_products}")
    print(f"Брой уникални типове: {unique_types}")
    print("\nТоп 10 най-често срещани типове:")
    for i, (p_type, count) in enumerate(sorted(type_counts.items(), key=lambda x: x[1], reverse=True)[:10], 1):
        print(f"{i}. {p_type}: {count} продукти ({(count/total_products*100):.1f}%)")
    
    return report_path


def update_product_types(df: pd.DataFrame) -> int:
    """
    Update product types based on the mappings from new-types.json
    
    Args:
        df: DataFrame containing the products data with 'Type' column
        
    Returns:
        Number of product types that were updated
    """
    # Path to the new-types.json file (in the project root)
    json_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'new-types.json')
    
    if not os.path.exists(json_path):
        print(f"  Внимание: Файлът new-types.json не е намерен на адрес: {json_path}")
        return 0
    
    try:
        # Load the type mappings
        with open(json_path, 'r', encoding='utf-8') as f:
            type_mappings = json.load(f)
        
        # Create a mapping dictionary (case-insensitive)
        type_map = {}
        for item in type_mappings:
            old_type = item.get('Type', '').strip()
            new_type = item.get('New Type', '').strip()
            if old_type and new_type and old_type.lower() != new_type.lower():
                type_map[old_type.lower()] = new_type
        
        if not type_map:
            print("  Внимание: Не са намерени валидни съпоставяния на типове в new-types.json")
            return 0
        
        # Track changes
        changes = {}
        
        # Update types in the DataFrame
        for idx, row in df.iterrows():
            current_type = str(row['Type']) if pd.notna(row['Type']) else ''
            if current_type and current_type.lower() in type_map:
                new_type = type_map[current_type.lower()]
                if current_type != new_type:
                    df.at[idx, 'Type'] = new_type
                    changes[current_type] = changes.get(current_type, 0) + 1
        
        # Print summary of changes
        if changes:
            print("\n  Справка за променените типове:")
            print("  " + "-" * 40)
            total_changes = 0
            for old_type, count in sorted(changes.items(), key=lambda x: x[1], reverse=True):
                new_type = type_map[old_type.lower()]
                print(f"  • {old_type} -> {new_type}: {count} продукта")
                total_changes += count
            print(f"  \n  Общо променени типове: {len(changes)}")
            print(f"  Общ брой продукти с променен тип: {total_changes}")
            
        return total_changes
        
    except Exception as e:
        print(f"  Грешка при обработка на new-types.json: {str(e)}")
        import traceback
        traceback.print_exc()
        return 0


def update_vendor_from_brand_export(df: pd.DataFrame, brand_file_path: str) -> int:
    """
    Update the Vendor column based on the product brand information from woocommerce-product-brand-export.xlsx.
    
    Args:
        df: DataFrame containing the products data
        brand_file_path: Path to the woocommerce-product-brand-export.xlsx file
        
    Returns:
        int: Number of products updated with vendor information
    """
    try:
        print(f"\n[DEBUG] Зареждане на файл с марки от: {brand_file_path}")
        # Read the brand export file
        brand_df = pd.read_excel(brand_file_path, engine='openpyxl')
        print(f"[DEBUG] Успешно зареден файл с марки. Брой редове: {len(brand_df)}")
        print(f"[DEBUG] Колони във файла с марки: {list(brand_df.columns)}")
        
        # Ensure required columns exist
        if 'Product ID' not in brand_df.columns or 'Product Brand' not in brand_df.columns:
            print("ГРЕШКА: Липсват задължителни колони във файла с марките. Очаквани колони: 'Product ID' и 'Product Brand'.")
            print(f"[DEBUG] Налични колони: {list(brand_df.columns)}")
            return 0
            
        # Create a dictionary mapping product IDs to brands
        print("\n[DEBUG] Създаване на речник за съпоставяне на ID към марка...")
        id_to_brand = dict(zip(
            brand_df['Product ID'].astype(str).str.strip(),
            brand_df['Product Brand'].astype(str).str.strip()
        ))
        
        # Debug: Print first 5 mappings
        print(f"[DEBUG] Първи 5 записа от речника (ID -> Brand): {dict(list(id_to_brand.items())[:5])}")
        
        # Initialize counter for updated products
        updated_count = 0
        
        # Find ONLY the exact WooCommerce ID column: 'Metafield: woo.id'
        woo_id_col = 'Metafield: woo.id' if 'Metafield: woo.id' in df.columns else None
        print(f"[DEBUG] Използвана колона за WooCommerce ID: {woo_id_col}")

        if woo_id_col is None:
            print("ВНИМАНИЕ: Не е намерена колона 'Metafield: woo.id'. Пропускане на актуализиране на марките.")
            return 0

        # Debug: Print first 5 WooCommerce IDs from the main DataFrame
        print(f"[DEBUG] Първи 5 WooCommerce ID от основния файл: {df[woo_id_col].head().tolist()}")
        print(f"[DEBUG] Брой уникални WooCommerce ID: {df[woo_id_col].nunique()}")
        print(f"[DEBUG] Брой празни WooCommerce ID: {df[woo_id_col].isna().sum()}")
        
        # Ensure Vendor column exists
        if 'Vendor' not in df.columns:
            df['Vendor'] = ''
        
        # Update Vendor column based on WooCommerce ID and brand mapping
        print("\n[DEBUG] Започва актуализиране на колоната Vendor...")
        
        # First, ensure we have a Vendor column
        if 'Vendor' not in df.columns:
            df['Vendor'] = ''
            print("[DEBUG] Създадена е нова колона 'Vendor'")

        # Helper to normalize IDs to comparable strings (e.g., 1147.0 -> '1147')
        def _norm_id(val):
            try:
                if pd.isna(val):
                    return ''
                # If numeric-like, cast to int then str
                if isinstance(val, (int,)):
                    return str(val)
                if isinstance(val, float):
                    return str(int(val))
                s = str(val).strip()
                # Remove trailing .0 if present
                if s.endswith('.0') and s.replace('.', '', 1).isdigit():
                    s = s[:-2]
                return s
            except Exception:
                return str(val).strip()

        # Normalize IDs in main DF and in the brand mapping
        df['_temp_woo_id'] = df[woo_id_col].apply(_norm_id)
        brand_mapping = pd.Series({ _norm_id(k): v for k, v in id_to_brand.items() })

        # Update only rows where we have a matching brand and the brand is non-empty/non-nan
        mask = df['_temp_woo_id'].isin(brand_mapping.index)
        print(f"[DEBUG] Намерени {int(mask.sum())} продукта със съвпадение в списъка с марки (след нормализация)")

        if mask.any():
            mapped = df.loc[mask, '_temp_woo_id'].map(brand_mapping)
            # Filter out empty or 'nan' string values
            valid = mapped.notna() & (mapped.astype(str).str.strip() != '') & (mapped.astype(str).str.lower() != 'nan') & (mapped.astype(str).str.lower() != 'none')
            df.loc[mask & valid, 'Vendor'] = mapped[valid]
            updated_count = int((mask & valid).sum())
            
            # Debug: Print some examples of updated vendors
            if updated_count > 0:
                sample = df[mask][['_temp_woo_id', 'Vendor']].head(3)
                print("[DEBUG] Примерни актуализирани марки:")
                for _, row in sample.iterrows():
                    print(f"  WooID: {row['_temp_woo_id']} -> Vendor: {row['Vendor']}")
        
        # Clean up the temporary column
        df.drop('_temp_woo_id', axis=1, inplace=True)
        
        print(f"[DEBUG] Актуализирани са общо {updated_count} продукта")
        
        return updated_count
        
    except Exception as e:
        print(f"ГРЕШКА при актуализиране на марките: {str(e)}")
        return 0
