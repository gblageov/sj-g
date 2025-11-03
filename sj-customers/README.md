# Shopify Customer Data Validator

Програма за валидация и поправка на липсващи данни в клиентски файлове за Shopify импорт.

## Функционалност

- Проверява XLSX файлове за липсващи данни в задължителните полета на Shopify
- Поправя автоматично липсващите данни:
  - Телефонни полета: попълват се с "+1234567890"
  - Всички останали полета: попълват се с "Shopify"
- Запазва нов файл с добавена дата и час в името
- Предоставя детайлен отчет за обработката

## Задължителни полета за проверка

### Customer
- Customer: Email

### Billing
- Billing: First Name
- Billing: Last Name
- Billing: Phone
- Billing: Address 1
- Billing: Country Code / Billing: Country

### Shipping
- Shipping: First Name
- Shipping: Last Name
- Shipping: Phone
- Shipping: Address 1
- Shipping: City
- Shipping: Country Code

## Изисквания

- Python 3.7+
- pandas
- openpyxl

## Инсталация

```bash
pip install pandas openpyxl
```

## Стартиране

```bash
python run.py
```

## Използване

1. Стартирайте програмата с `python run.py`
2. Натиснете "Browse..." за да изберете XLSX файл
3. Натиснете "Validate & Fix Data" за да обработите файла
4. Резултатът ще бъде запазен в нов файл с добавена дата и час
5. Конзолата ще покаже детайлен отчет за обработката

## Изходни файлове

- **Excel файл**: Поправените данни със същото име + дата_час
- **Текст файл**: Лог на обработката със същото име + дата_час
