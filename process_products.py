import pandas as pd
import json
import ast
import re
import os
import sys

# Глобална променлива за името на целевата колона
TARGET_METAFIELD_COLUMN = 'Metafield: shopify_migration_connectet_products'

# --- КОНФИГУРАЦИЯ ЗА ДЕБЪГВАНЕ ---
DEBUG_ITEMS = {} 

def process_woocommerce_to_shopify(file_path):
    if not os.path.exists(file_path):
        print(f"ГРЕШКА: Файлът не е намерен на адрес: {file_path}")
        return None
    
    try:
        # Потискаме предупреждението от openpyxl при четене
        df = pd.read_excel(file_path, sheet_name='Products', engine='openpyxl')
        print(f"Файлът '{file_path}' е прочетен успешно. Общо редове: {len(df)}")
    except Exception as e:
        print(f"ГРЕШКА при четене на Excel файла: {e}")
        return None
    
    required_columns = [
        'Metafield: woo.woobt_ids', 'Variant SKU', 'Handle', 
        TARGET_METAFIELD_COLUMN, 'Metafield: woo.id', 'Variant Metafield: woo.id'
    ]
    for col in required_columns:
        if col not in df.columns:
            print(f"ГРЕШКА: Липсва задължителна колона '{col}' във файла.")
            return None

    # <<< НОВО: РЕШЕНИЕ ЗА FutureWarning >>>
    # Изрично задаваме типа на целевата колона като 'object' (за текст),
    # за да избегнем предупреждението за несъвместим тип данни.
    df[TARGET_METAFIELD_COLUMN] = df[TARGET_METAFIELD_COLUMN].astype(object)

    sku_to_handle = {}
    woo_id_to_handle = {}
    last_valid_handle = '' 

    print("\nЗапочва създаване на речници за търсене...")
    # ... (останалата част от кода остава НАПЪЛНО НЕПРОМЕНЕНА) ...
    for idx, row in df.iterrows():
        if pd.notna(row['Handle']) and str(row['Handle']).strip() != '':
            last_valid_handle = str(row['Handle']).strip()

        if not last_valid_handle:
            continue

        variant_sku = row['Variant SKU']
        if pd.notna(variant_sku) and str(variant_sku).strip() != '':
            sku_str = str(variant_sku).strip()
            sku_to_handle[sku_str] = last_valid_handle

        id_to_process = None
        main_woo_id = row['Metafield: woo.id']
        
        if pd.notna(main_woo_id) and str(main_woo_id).strip() != '':
            id_to_process = main_woo_id
        else:
            variant_woo_id = row['Variant Metafield: woo.id']
            if pd.notna(variant_woo_id) and str(variant_woo_id).strip() != '':
                id_to_process = variant_woo_id
        
        if id_to_process:
            try:
                id_str = str(int(float(id_to_process)))
                woo_id_to_handle[id_str] = last_valid_handle
                
                if id_str in DEBUG_ITEMS:
                    print(f"[ДЕБЪГ | РЕЧНИК] ID '{id_str}' е асоцииран с Handle: '{last_valid_handle}' (от ред {idx + 2})")
            except (ValueError, TypeError):
                continue

    print(f"-> Създаден е речник с {len(sku_to_handle)} уникални SKU-та.")
    print(f"-> Създаден е речник с {len(woo_id_to_handle)} уникални Woo ID-та (от двата източника).")

    rows_with_woobt_data = df['Metafield: woo.woobt_ids'].notna().sum()
    print(f"--> Намерени са общо {rows_with_woobt_data} реда с данни в 'Metafield: woo.woobt_ids', които ще бъдат обработени.")
    
    updated_count = 0
    rows_with_data_count = 0
    json_parse_errors = []
    unmatched_products = []

    print("\nЗапочва обработка на редовете...")
    for idx, row in df.iterrows():
        woobt_ids = row['Metafield: woo.woobt_ids']
        
        if pd.isna(woobt_ids) or str(woobt_ids).strip() == '':
            continue
            
        rows_with_data_count += 1
        
        print(f"Обработване на ред {rows_with_data_count} от {rows_with_woobt_data}...", end='\r')
        sys.stdout.flush()
        
        excel_row_num = idx + 2
        
        try:
            woobt_str = str(woobt_ids)
            woobt_str = re.sub(r'^[^{]*({.*})[^}]*$', r'\1', woobt_str)
            
            woobt_data = None
            try:
                woobt_data = json.loads(woobt_str)
            except:
                try:
                    woobt_data = ast.literal_eval(woobt_str)
                except:
                    json_parse_errors.append(f"Ред {excel_row_num}: Неуспешно разчитане на JSON -> '{woobt_str}'")
                    continue
            
            if not isinstance(woobt_data, dict):
                continue

            products_data = []
            for key in woobt_data:
                if isinstance(woobt_data[key], dict):
                    products_data.append({
                        'sku': str(woobt_data[key].get('sku', '')).strip(),
                        'id': str(woobt_data[key].get('id', '')).strip()
                    })
            
            if not products_data:
                continue

            matching_handles = []
            row_unmatched_products = []
            
            for product in products_data:
                sku = product['sku']
                product_id = product['id']
                found_handle = None
                
                is_debug_target = sku in DEBUG_ITEMS or product_id in DEBUG_ITEMS
                if is_debug_target:
                    print("\n" + "-"*20 + f" ДЕБЪГ НА РЕД {excel_row_num} " + "-"*20)
                    print(f"Търси се -> SKU: '{sku}', ID: '{product_id}'")
                
                if sku and sku in sku_to_handle:
                    found_handle = sku_to_handle[sku]
                    if is_debug_target: print(f"  [OK] Намерен по SKU. Handle: '{found_handle}'")
                elif product_id and product_id in woo_id_to_handle:
                    found_handle = woo_id_to_handle[product_id]
                    if is_debug_target: print(f"  [OK] Намерен по ID. Handle: '{found_handle}'")
                
                if found_handle:
                    matching_handles.append(found_handle)
                else:
                    if is_debug_target: 
                        print(f"  [ГРЕШКА] Продуктът не е намерен нито по SKU, нито по ID.")
                        print(f"    -> Проверка за SKU '{sku}' в речника: {sku in sku_to_handle}")
                        print(f"    -> Проверка за ID '{product_id}' в речника: {product_id in woo_id_to_handle}")
                        print("-" * (44 + len(str(excel_row_num))))
                    row_unmatched_products.append(f"SKU: '{sku}'/ID: '{product_id}'")

            if row_unmatched_products:
                unmatched_products.append(f"Ред {excel_row_num}: Не са намерени съвпадения за -> {', '.join(row_unmatched_products)}")

            if matching_handles:
                df.at[idx, TARGET_METAFIELD_COLUMN] = ','.join(list(set(matching_handles)))
                updated_count += 1
                
        except Exception as e:
            print(f"Критична грешка при обработка на ред {excel_row_num}: {e}")
            continue
    
    print() 
    
    # <<< ПРОМЯНА 2: ТОВА Е НОВАТА ИНДИКАЦИЯ >>>
    print("\nОбработката на редовете приключи. Започва запис на новия Excel файл...")
    print("Тази стъпка може да отнеме известно време, моля изчакайте...")

    # Диагностичен доклад
    output_path = file_path.replace('.xlsx', '_updated.xlsx')
    df.to_excel(output_path, index=False, sheet_name='Products', engine='xlsxwriter')
    
    print("\n" + "="*50)
    print("ОБРАБОТАТА ПРИКЛЮЧИ - ДИАГНОСТИЧЕН ДОКЛАД")
    # ... останалата част от кода...

# ... (кодът продължава без промяна)
    print("="*50)
    print(f"Общо намерени редове с данни в 'Metafield: woo.woobt_ids': {rows_with_data_count}")
    print(f"Успешно обновени редове в '{TARGET_METAFIELD_COLUMN}': {updated_count}")
    print(f"Редове с грешка при разчитане на JSON данните: {len(json_parse_errors)}")
    print(f"Редове с продукти, ненамерени по никой от критериите: {len(unmatched_products)}")
    print("-"*50)

    if unmatched_products:
        print("\nПЪЛЕН СПИСЪК НА НЕНАМЕРЕНИТЕ ПРОДУКТИ:")
        for error in unmatched_products:
            print(error)
            
    print(f"\nОбновеният файл е запазен като: {output_path}")
    print("="*50)
    
    return output_path

if __name__ == "__main__":
    file_path = 'import_result.xlsx'
    process_woocommerce_to_shopify(file_path)