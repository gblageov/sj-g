# Конвертор WooCommerce → Shopify

## Цел на програмата
Инструмент за обработка на Excel експорт от WooCommerce към формата за импорт в Shopify. Предлага графичен интерфейс (Tkinter) и CLI. Извършва почистване на колони, попълване на типове продукти, изграждане на речници (SKU → Handle, Woo ID → Handle), копиране на допълнителни Excel листове и запис на финален файл.

## Структура
- gui.py — Графичен интерфейс (Tkinter)
- run.py — CLI входна точка
- processing/
  - pipeline.py — Основен оркестратор `process_woocommerce_to_shopify`
  - io.py — Вход/изход и помощни функции за Excel
  - type_detection.py — Откриване на тип продукт
  - mapping.py — Изграждане на речници за търсене
  - parsing.py — Парсване на JSON-подобни стойности
  - report.py — Обобщаващ отчет
  - constants.py — Константи (типове продукти, целеви колони, преименувания)

## Входни точки
- GUI: функция `main()` в `gui.py` (стартира `ShopifyConverterApp`)
- CLI: `python run.py -i import_result.xlsx`

## Основен поток: processing/pipeline.py
- **`process_woocommerce_to_shopify(file_path, output_file=None)`**
  - Чете Products лист чрез `io.read_products_df`
  - Премахва колони съдържащи `Metafield: woo.xts-blocks` с `io.remove_xts_blocks_columns`
  - Попълва колона `Type` чрез `type_detection.populate_type_column`
  - Опитва актуализация на `Vendor` спрямо `data/woocommerce-product-brand-export.xlsx` (чрез вътрешна функция `update_vendor_from_brand_export` и `update_product_types` — налични в pipeline.py)
  - Изгражда речници `SKU → Handle` и `Woo ID → Handle` с `mapping.build_sku_to_handle` и `mapping.build_woo_id_to_handle`
  - Генерира диагностични съобщения
  
Пример (извадка):
```python
# Импортира локалния модул io от текущия пакет и го именува като io_mod,
# за да извикваме функции като io_mod.read_products_df, io_mod.write_products_df и др.
from . import io as io_mod

# Импортира функцията, която попълва колоната 'Type' според 'Title'
# на продукта и списъка с допустими типове (PRODUCT_TYPES).
from .type_detection import populate_type_column

# Импортира функции за изграждане на речници за търсене:
# - SKU -> Handle (за бързо намиране на handle по SKU)
# - Woo ID -> Handle (за бързо намиране на handle по WooCommerce ID)
from .mapping import build_sku_to_handle, build_woo_id_to_handle

# Четене на входния файл
df = io_mod.read_products_df(file_path)

# Премахване на XTS блокове
df, removed_count = io_mod.remove_xts_blocks_columns(df)

# Попълване на колона Type
types_added_count = populate_type_column(df, PRODUCT_TYPES)

# Речници за Combined handle
sku_to_handle = build_sku_to_handle(df)
woo_id_to_handle = build_woo_id_to_handle(df)
```

## Модул processing/io.py
- **`read_products_df(file_path)`**
  - Чете `Products` лист (engine=openpyxl)
  - Прилага преименувания от `COLUMN_RENAMES`
  - Нормализира pipe-стойности към JSON списък чрез `process_column_value`
  - Нормализира `woo.bgfd_enable_product_engraving` към текстови `True`/`False`
  - Гарантира наличие и позиция на `Metafield: global.Combined handle` и `Type`
  - Проверява задължителни колони и типове
  
Пример:
```python
df = pd.read_excel(file_path, sheet_name='Products', engine='openpyxl')
# Преименувания
renames_to_apply = {old: new for old, new in COLUMN_RENAMES.items() if old in df.columns and new not in df.columns}
if renames_to_apply:
    df.rename(columns=renames_to_apply, inplace=True)

# Pipe -> JSON масив
for col in df.columns:
    if col in COLUMN_RENAMES.values() or col in COLUMN_RENAMES.keys():
        df[col] = df[col].apply(process_column_value)
```

- **`process_column_value(value)`**: Превръща `"a|b|c"` в JSON `["a","b","c"]`; празни стойности → `''`.

- **`remove_xts_blocks_columns(df)`**: Премахва колони съдържащи `'Metafield: woo.xts-blocks'`.

- **`copy_other_sheets(input_path, output_path)`**: Копира всички листове, различни от `prodct` и `Products`.

- **`write_products_df(df, output_path, input_path=None)`**: Записва `Products` и копира останалите листове от входния файл.

## Модул processing/type_detection.py
- **`infer_type_from_title(title, product_types)`**: намира най-дългото съвпадение в началото на заглавието.

Пример:
```python
title_normalized = re.sub(r'\s+', ' ', str(title).strip()).lower()
if title_normalized.startswith(pt_lower):
    found_matches.append(product_type)
```

- **`populate_type_column(df, product_types)`**: попълва `df['Type']` и връща брой добавени типове.

## Модул processing/mapping.py
- **`build_sku_to_handle(df)`**: итерира редовете, пази `last_valid_handle`, създава `SKU → Handle`.
- **`build_woo_id_to_handle(df)`**: предпочита `Metafield: woo.id`, пада към `Variant Metafield: woo.id`, нормализира ID до int-стринг.

Пример:
```python
if pd.notna(variant_sku) and str(variant_sku).strip() != '':
    sku_to_handle[str(variant_sku).strip()] = last_valid_handle

raw_id = row.get('Metafield: woo.id') or row.get('Variant Metafield: woo.id')
norm_id = str(int(float(raw_id)))
woo_id_to_handle[norm_id] = last_valid_handle
```

## Модул processing/parsing.py
- **`normalize_woobt_string(raw)`**: извлича JSON блок от шумен стринг чрез регекс.
- **`extract_woobt_dict(raw)`**: първо пробва `json.loads`, после `ast.literal_eval`; връща `(dict|None, error|None)`.

Пример:
```python
try:
    data = json.loads(cleaned)
    if isinstance(data, dict):
        return data, None
except Exception:
    pass
```

## Модул processing/report.py
- **`print_summary_report(...)`**: визуализира числа за добавени типове, обновени редове, грешки, пълен списък на ненамерени продукти и път към изходния файл.

## Модул processing/constants.py
- **`PRODUCT_TYPES`**: списък с типове продукти (на български).
- **`TARGET_METAFIELD_COLUMN`**: `'Metafield: global.Combined handle'`.
- **`COLUMN_RENAMES`**: речник за преименувания от български към английски атрибути.

## GUI: gui.py
- **`ShopifyConverterApp`**
  - UI за избор на файл, бутон “Run Conversion”, конзолен изход в прозорец
  - `start_conversion()` стартира нишка
  - `run_conversion(input_file)` изчислява изходни имена, прави Tee на stdout/stderr и извиква `process_woocommerce_to_shopify`

Пример:
```python
sys.stdout = TeeOutput(original_stdout, f)
sys.stderr = TeeOutput(original_stderr, f)
output = process_woocommerce_to_shopify(input_file, output_file=output_excel)
```

- **`main()`**: създава `Tk()`, инициализира тема и стартира `mainloop()`.

## CLI: run.py
- **`main()`**: `argparse -i/--input`, извиква `process_woocommerce_to_shopify` и връща exit code 0/1.

## Как да стартирате
- GUI: `python gui.py`
- CLI: `python run.py -i път/до/import_result.xlsx`

## Изисквания
- Python 3.x, openpyxl, pandas, xlsxwriter
- Входният Excel да съдържа лист `Products` и задължителните колони, описани в `io.read_products_df`
