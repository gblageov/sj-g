import pandas as pd
import json
import ast
import re
import os
import sys

# СПИСЪК С ТИПОВЕ ПРОДУКТИ
PRODUCT_TYPES = [
  "Комплект", "Медальон", "Обеци", "Асиметрични обеци", "Брошка /медальон",
  "Брошка", "Брошка/медальон", "Ваучер за подарък", "Годежен сребърен пръстен",
  "Голяма луксозна кутия", "Голяма подаръчна торбичка", "Гривна", "Гривна за глезен",
  "Гривна за глезен monette", "Гривна снежинка", "Детска златна гривна",
  "Детска сребърна гривна", "Детски сребърни обеци", "Елегантна сребърна гривна",
  "Заключващи се винтчета за обеци", "Златен годежен пръстен", "Златен кръст",
  "Златен пръстен", "Златен синджир", "Златна гривна", "Златни обеци",
  "Златни обеци с диаманти", "Златно колие", "Златно колие с диаманти", "Картичка",
  "Колие", "Колие със султанит", "Комплект гривни", "Комплект с яспис",
  "Комплект със султанит", "Кръст", "Кутия за бижута", "Луксозна кутия за комплект",
  "Малка луксозна кутия", "Малка подаръчна торбичка", "Медальон камея",
  "Медальон лунен камък", "Медальон/брошка", "Медальон/брошка mapple leaf",
  "Обеци пчели", "Обеци халки", "Обеци халки angel whisper", "Подаръчна торбичка",
  "Пръстен", "Пръстен лунен камък", "Пръстен с лунен камък", "Пръстен султанит",
  "Пръстен танзанит", "Романтично сребърно колие", "Сребърен годежен пръстен с брилянт",
  "Сребърен двоен пръстен", "Сребърен компкект", "Сребърен комплект",
  "Сребърен комплект dorian", "Сребърен комплект снежинки", "Сребърен комплект султанит",
  "Сребърен кръст", "Сребърен кръст с брилянт", "Сребърен кръст с позлата и брилянт",
  "Сребърен медальон", "Сребърен медальон angel heart", "Сребърен медальон ангел",
  "Сребърен медальон ключ", "Сребърен медальон корона", "Сребърен медальон майка",
  "Сребърен медальон с брилянт", "Сребърен медальон фея", "Сребърен медальон/брошка",
  "Сребърен православен руски кръст", "Сребърен пръстен", "Сребърен пръстен meteorite",
  "Сребърен пръстен за фаланга", "Сребърен пръстен змия", "Сребърен пръстен марказит",
  "Сребърен пръстен оникс", "Сребърен пръстен с брилянт", "Сребърен пръстен с гранат",
  "Сребърен пръстен с изумруд", "Сребърен пръстен с родиево покритие и цирконий",
  "Сребърен пръстен самолет", "Сребърен пръстен султанит", "Сребърен пръстен часовник",
  "Сребърен сгъваем пръстен", "Сребърен синджир", "Сребърен синджир кардинал",
  "Сребърен часовник", "Сребърна брошка", "Сребърна брошка / медальон",
  "Сребърна брошка/пин", "Сребърна виличка", "Сребърна гривна",
  "Сребърна гривна за глезен", "Сребърна гривна с брилянт", "Сребърна гривнаm",
  "Сребърна камбанка", "Сребърна лъжичка", "Сребърна обеца", "Сребърна твърда гривна",
  "Сребърна чаша", "Сребърни обеци", "Сребърни обеци moon dance",
  "Сребърни обеци звезди", "Сребърни обеци капка", "Сребърни обеци керамика",
  "Сребърни обеци корона", "Сребърни обеци марказит", "Сребърни обеци панделки",
  "Сребърни обеци планети", "Сребърни обеци пчели", "Сребърни обеци с брилянт",
  "Сребърни обеци с брилянти", "Сребърни обеци с диаманти", "Сребърни обеци с корал",
  "Сребърни обеци с перла", "Сребърни обеци с перли", "Сребърни обеци с рубин",
  "Сребърни обеци с топаз", "Сребърни обеци сърца", "Сребърни обеци топчета",
  "Сребърни обеци фиба", "Сребърни обеци халки", "Сребърни обеци – white jasmine s",
  "Сребърно двойно колие", "Сребърно коли", "Сребърно колие", "Сребърно колие ангел",
  "Сребърно колие ангелско крило", "Сребърно колие детелина", "Сребърно колие дървото на живота",
  "Сребърно колие еленче", "Сребърно колие звезда", "Сребърно колие капчица",
  "Сребърно колие ключ", "Сребърно колие краче", "Сребърно колие мече",
  "Сребърно колие перо", "Сребърно колие пчела", "Сребърно колие ръката на фатима",
  "Сребърно колие с брилянт", "Сребърно колие с брилянт – my little girl",
  "Сребърно колие снежинка", "Сребърно колие сърца", "Сребърно колие сърце",
  "Твърда сребърна гривна", "Тройно сребърно колие"
]

# Глобална променлива за името на целевата колона
TARGET_METAFIELD_COLUMN = 'Metafield: global.Combined handle'

# --- КОНФИГУРАЦИЯ ЗА ДЕБЪГВАНЕ ---
DEBUG_ITEMS = {} 

def process_woocommerce_to_shopify(file_path):
    if not os.path.exists(file_path):
        print(f"ГРЕШКА: Файлът не е намерен на адрес: {file_path}")
        return None
    
    try:
        df = pd.read_excel(file_path, sheet_name='Products', engine='openpyxl')
        print(f"Файлът '{file_path}' е прочетен успешно. Общо редове: {len(df)}")
    except Exception as e:
        print(f"ГРЕШКА при четене на Excel файла: {e}")
        return None
    
    # --- СЪЗДАВАНЕ НА КОЛОНА 'Metafield: global.Combined handle' ---
    if TARGET_METAFIELD_COLUMN not in df.columns:
        print(f"Забележка: Целевата колона '{TARGET_METAFIELD_COLUMN}' липсва. Тя ще бъде създадена автоматично.")
        try:
            reference_col_index = df.columns.get_loc('Metafield: woo.woobt_ids')
            df.insert(loc=reference_col_index, column=TARGET_METAFIELD_COLUMN, value='')
            print(f"-> Колоната '{TARGET_METAFIELD_COLUMN}' е успешно създадена.")
        except KeyError:
            print(f"ГРЕШКА: Референтната колона 'Metafield: woo.woobt_ids' не е намерена.")
            return None
    
    # --- СЪЗДАВАНЕ НА КОЛОНА 'Type' ---
    if 'Type' not in df.columns:
        print(f"Забележка: Целевата колона 'Type' липсва. Тя ще бъде създадена автоматично.")
        try:
            title_col_index = df.columns.get_loc('Title')
            df.insert(loc=title_col_index + 1, column='Type', value='')
            print(f"-> Колоната 'Type' е успешно създадена след 'Title'.")
        except KeyError:
            print(f"ГРЕШКА: Задължителната колона 'Title' не е намерена, за да се добави колона 'Type'.")
            return None

    # Проверка за всички задължителни колони
    required_columns = [
        'Metafield: woo.woobt_ids', 'Variant SKU', 'Handle', 
        TARGET_METAFIELD_COLUMN, 'Metafield: woo.id', 'Variant Metafield: woo.id', 'Title'
    ]
    for col in required_columns:
        if col not in df.columns:
            print(f"ГРЕШКА: Липсва задължителна колона '{col}' във файла.")
            return None

    df[TARGET_METAFIELD_COLUMN] = df[TARGET_METAFIELD_COLUMN].astype(object)

    # --- КОРЕГИРАНА ЛОГИКА ЗА ПОПЪЛВАНЕ НА КОЛОНА 'Type' ---
    print("\nЗапочва попълване на колона 'Type' (търсене на най-пълно съвпадение)...")
    types_added_count = 0

    for idx, row in df.iterrows():
        title = str(row['Title']).strip()
        if not title:
            continue
        
        # Преобразуваме заглавието в малки букви веднъж за ефективност
        title_lower = title.lower()
        
        found_matches = []
        # Обхождаме всички типове, за да намерим всички възможни съвпадения
        for product_type in PRODUCT_TYPES:
            # Сравняваме с малки букви, за да игнорираме регистъра
            if title_lower.startswith(product_type.lower()):
                found_matches.append(product_type)
        
        # Ако сме намерили поне едно съвпадение, избираме най-дългото от тях
        if found_matches:
            best_match = max(found_matches, key=len)
            df.at[idx, 'Type'] = best_match
            types_added_count += 1

    print(f"-> Попълването приключи. Добавени са {types_added_count} типа в колона 'Type'.")

    # --- СЪЩЕСТВУВАЩА ЛОГИКА ЗА 'Combined handle' ---
    sku_to_handle = {}
    woo_id_to_handle = {}
    last_valid_handle = '' 

    print("\nЗапочва създаване на речници за търсене за 'Combined handle'...")
    for idx, row in df.iterrows():
        if pd.notna(row['Handle']) and str(row['Handle']).strip() != '':
            last_valid_handle = str(row['Handle']).strip()
        if not last_valid_handle: continue
        variant_sku = row['Variant SKU']
        if pd.notna(variant_sku) and str(variant_sku).strip() != '':
            sku_to_handle[str(variant_sku).strip()] = last_valid_handle
        id_to_process = row['Metafield: woo.id'] if pd.notna(row['Metafield: woo.id']) and str(row['Metafield: woo.id']).strip() != '' else row['Variant Metafield: woo.id']
        if pd.notna(id_to_process) and str(id_to_process).strip() != '':
            try:
                woo_id_to_handle[str(int(float(id_to_process)))] = last_valid_handle
            except (ValueError, TypeError): continue

    print(f"-> Създаден е речник с {len(sku_to_handle)} уникални SKU-та.")
    print(f"-> Създаден е речник с {len(woo_id_to_handle)} уникални Woo ID-та.")

    rows_with_woobt_data = df['Metafield: woo.woobt_ids'].notna().sum()
    print(f"--> Намерени са {rows_with_woobt_data} реда с данни в 'Metafield: woo.woobt_ids', които ще бъдат обработени.")
    
    updated_count = 0; rows_with_data_count = 0; json_parse_errors = []; unmatched_products = []

    print("\nЗапочва обработка на 'Combined handle'...")
    for idx, row in df.iterrows():
        woobt_ids = row['Metafield: woo.woobt_ids']
        if pd.isna(woobt_ids) or str(woobt_ids).strip() == '': continue
        rows_with_data_count += 1
        print(f"Обработване на ред {rows_with_data_count} от {rows_with_woobt_data}...", end='\r'); sys.stdout.flush()
        excel_row_num = idx + 2
        try:
            woobt_str = re.sub(r'^[^{]*({.*})[^}]*$', r'\1', str(woobt_ids))
            try: woobt_data = json.loads(woobt_str)
            except:
                try: woobt_data = ast.literal_eval(woobt_str)
                except: json_parse_errors.append(f"Ред {excel_row_num}: Неуспешно разчитане на JSON -> '{woobt_str}'"); continue
            if not isinstance(woobt_data, dict): continue
            products_data = [{'sku': str(v.get('sku', '')).strip(), 'id': str(v.get('id', '')).strip()} for k, v in woobt_data.items() if isinstance(v, dict)]
            if not products_data: continue
            matching_handles = []; row_unmatched_products = []
            for product in products_data:
                sku, product_id = product['sku'], product['id']
                found_handle = sku_to_handle.get(sku) or woo_id_to_handle.get(product_id)
                if found_handle: matching_handles.append(found_handle)
                else: row_unmatched_products.append(f"SKU: '{sku}'/ID: '{product_id}'")
            if row_unmatched_products: unmatched_products.append(f"Ред {excel_row_num}: Не са намерени съвпадения за -> {', '.join(row_unmatched_products)}")
            if matching_handles: df.at[idx, TARGET_METAFIELD_COLUMN] = ','.join(list(set(matching_handles))); updated_count += 1
        except Exception as e: print(f"Критична грешка при обработка на ред {excel_row_num}: {e}"); continue
    
    print() 
    print("\nОбработката на редовете приключи. Започва запис на новия Excel файл...")
    print("Тази стъпка може да отнеме известно време, моля изчакайте...")

    output_path = file_path.replace('.xlsx', '_updated.xlsx')
    df.to_excel(output_path, index=False, sheet_name='Products', engine='xlsxwriter')
    
    print("\n" + "="*50)
    print("ОБРАБОТАТА ПРИКЛЮЧИ - ДИАГНОСТИЧЕН ДОКЛАД")
    print("="*50)
    print(f"Успешно добавени типове в колона 'Type': {types_added_count}")
    print("-" * 50)
    print(f"Общо намерени редове с данни в 'Metafield: woo.woobt_ids': {rows_with_data_count}")
    print(f"Успешно обновени редове в '{TARGET_METAFIELD_COLUMN}': {updated_count}")
    print(f"Редове с грешка при разчитане на JSON данните: {len(json_parse_errors)}")
    print(f"Редове с продукти, ненамерени по никой от критериите: {len(unmatched_products)}")
    print("-"*50)

    if unmatched_products:
        print("\nПЪЛЕН СПИСЪК НА НЕНАМЕРЕНИТЕ ПРОДУКТИ:")
        for error in unmatched_products: print(error)
            
    print(f"\nОбновеният файл е запазен като: {output_path}")
    print("="*50)
    
    return output_path

if __name__ == "__main__":
    file_path = 'import_result.xlsx'
    process_woocommerce_to_shopify(file_path)