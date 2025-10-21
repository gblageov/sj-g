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


def process_woocommerce_to_shopify(file_path: str) -> Optional[str]:
    """
    Orchestrates the processing pipeline equivalent to the original implementation.
    Returns the output file path on success, or None on error.
    """
    df = io_mod.read_products_df(file_path)
    if df is None:
        return None

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

    base, ext = os.path.splitext(file_path)
    ts = datetime.now().strftime('%Y%m%d-%H%M')
    output_path = f"{base}_updated_{ts}{ext if ext else '.xlsx'}"
    io_mod.write_products_df(df, output_path)

    print_summary_report(
        types_added_count=types_added_count,
        rows_with_data_count=rows_with_data_count,
        updated_count=updated_count,
        json_parse_errors=json_parse_errors,
        unmatched_products=unmatched_products,
        output_path=output_path,
    )

    return output_path
