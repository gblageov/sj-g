# Валидатор на клиентски данни за Shopify

## Цел на програмата
Графично приложение и скрипт за валидиране и корекция на клиентски данни (Excel), за да бъдат готови за импорт в Shopify. Открива липсващи задължителни полета, попълва ги на базата на налична информация (включително групиране по поръчки и „Top Row“), нормализира стойности (TRUE/FALSE), добавя таг, и записва резултат в нов Excel файл, като пази останалите листове.

## Структура
- sj-customers/run.py — CLI входна точка (стартира GUI `main()`)
- sj-customers/gui.py — Графичен интерфейс `CustomerValidatorApp`
- sj-customers/processing/customer_validator.py — Бизнес логика за валидиране и корекция

## Входни точки
- GUI: `python sj-customers/gui.py` или `python sj-customers/run.py`
- CLI (непълен, основният поток е в GUI): самата обработка е в `process_customer_file()` и може да бъде извикана програмно

## Графичен интерфейс: sj-customers/gui.py
- **`CustomerValidatorApp`**
  - Поле за избор на Excel файл, бутон “Validate & Fix Data”, вграден конзолен прозорец.
  - `browse_file()` — отваря диалог за избор на `.xlsx` файл.
  - `start_validation()` — проверява пътя, деактивира бутона, стартира нишка.
  - `run_validation(input_file)` — генерира изходни имена с timestamp, Tee на stdout/stderr към GUI и .txt лог, извиква `process_customer_file`.

Пример (извадка):
```python
class TeeOutput:
    def __init__(self, *files):
        self.files = files
    def write(self, obj):
        for f in self.files:
            f.write(obj)

output = process_customer_file(input_file, output_file=output_excel)
```

- **`main()`** — създава `Tk()`, инициализира тема и стартира `mainloop()`.

## Бизнес логика: sj-customers/processing/customer_validator.py
- **`get_order_groups(df) -> Dict[str, Dict]`**
  - Изисква колони `Top Row` и `Name`.
  - Групира редовете по `Name`. „Top Row“ сочи реда-източник за групата.
  - Връща речник: `{ order_name: { top_row_index, row_indices } }`.

Пример:
```python
top_rows = df[(df['Top Row'].astype(str).str.strip() != '')]
for _, top_row in top_rows.iterrows():
    order_name = str(top_row['Name']).strip()
    order_rows = df[name_series == order_name].index.tolist()
    order_groups[order_name] = {
        'top_row_index': top_row.name,
        'row_indices': order_rows,
    }
```

- **`propagate_order_data(df, order_groups) -> pd.DataFrame`**
  - За всяка група копира липсващи стойности от Top Row към дефиниран набор полета (email, имена, адреси, град, държава/код).
  - Специална логика за телефони: намира първи наличен телефон сред стандартни и Woo полета на Top Row, после го разпространява към `Billing: Phone` и `Shipping: Phone` в групата.

Пример (телефони):
```python
all_phone_cols = ['Billing: Phone', 'Shipping: Phone', 'Metafield: woo._billing_tel', 'Metafield: woo.billing_tel']
# намиране на top_phone
for pcol in all_phone_cols:
    val = top_row.get(pcol)
    if val not in [None, '']:
        top_phone = val; break
# попълване
for phone_field in ['Billing: Phone', 'Shipping: Phone']:
    current_val = df.at[row_idx, phone_field]
    if not current_val or str(current_val).strip() == '':
        df.at[row_idx, phone_field] = top_phone
```

- **`process_customer_file(input_file, output_file=None) -> Optional[str]`**
  - Чете всички листове, изисква да има лист `Orders`, зарежда го като текст (`dtype=str`, `keep_default_na=False`).
  - Подменя `Command: NEW → MERGE` (ако колоната съществува).
  - Ако има `Top Row`: формира групи и разпространява данни чрез `get_order_groups` и `propagate_order_data`.
  - Превръща „true/false“ стойности до главни `TRUE/FALSE` за всички текстови колони.
  - Търси първия празен ред по `Name` и отрязва данните след него.
  - Проверява наличие на задължителни полета и отчита липсващите.
  - Попълва липсващи стойности с правила:
    - `Customer: Email` → `shopify@getnada.com` (ако липсва).
    - Телефони: използва top row, после кръстосани полета, накрая задава `+359-888888888`.
    - Имена/Адрес/Град: кръстосано попълване между Billing/Shipping, иначе стойност `Shopify`.
    - Държава/Код: `Bulgaria`/`BG` по подразбиране.
  - Добавя таг `judgeme_excluded` към колоната `Tags` (създава я при липса).
  - Запазва всички листове в новия файл, като подменя `Orders` с обработения `df`.
  - Генерира подробен SUMMARY в конзолата (вкл. статистики преди/след, оставащи липси, shipping данни).

Пример (част от попълване):
```python
elif col in ['Billing: Address 1', 'Shipping: Address 1']:
    other = 'Shipping: Address 1' if col == 'Billing: Address 1' else 'Billing: Address 1'
    mask = (df[col].astype(str).str.strip() == '') & (df[other].astype(str).str.strip() != '')
    df.loc[mask, col] = df.loc[mask, other]
    df[col] = df[col].fillna('Shopify').replace('', 'Shopify')
```

Пример (запазване на всички листове):
```python
xl = pd.ExcelFile(input_file, engine='openpyxl')
sheet_data = {name: (df if name == 'Orders' else pd.read_excel(xl, sheet_name=name))
              for name in xl.sheet_names}
with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    for sheet_name, sheet_df in sheet_data.items():
        sheet_df.to_excel(writer, index=False, sheet_name=sheet_name)
```

## Логване и отчет
- Подробни принтове за всеки етап: четене, колони, липсващи полета, броячи за фиксовете, статистики преди/след.
- SUMMARY REPORT с ключови числа: общо редове, намерени/липсващи колони, брой редове с липси, общо корекции, оставащи липси, липсващи shipping държава/код.
- Логът се записва и в `.txt` файл до Excel изхода (през GUI Tee).

## Как да използвате
- Стартирайте GUI и изберете входния Excel с лист `Orders`:
  - `python sj-customers/gui.py`
  - или `python sj-customers/run.py`
- Резултат:
  - Изходен Excel: `<оригинално_име>_YYYYMMDD_HHMMSS.xlsx`
  - Лог файл: `<оригинално_име>_YYYYMMDD_HHMMSS.txt`

## Изисквания
- Python 3.x, openpyxl, pandas
- Входният Excel трябва да съдържа лист `Orders` и препоръчително колоните, изброени в `required_fields` на `process_customer_file`.
