def print_summary_report(types_added_count: int,
                         rows_with_data_count: int,
                         updated_count: int,
                         json_parse_errors: list,
                         unmatched_products: list,
                         output_path: str):
    print("\n" + "="*50)
    print("ОБРАБОТАТА ПРИКЛЮЧИ - ДИАГНОСТИЧЕН ДОКЛАД")
    print("="*50)
    print(f"Успешно добавени типове в колона 'Type': {types_added_count}")
    print("-" * 50)
    print(f"Общо намерени редове с данни в 'Metafield: woo.woobt_ids': {rows_with_data_count}")
    print(f"Успешно обновени редове в 'Metafield: global.Combined handle': {updated_count}")
    print(f"Редове с грешка при разчитане на JSON данните: {len(json_parse_errors)}")
    print(f"Редове с продукти, ненамерени по никой от критериите: {len(unmatched_products)}")
    print("-"*50)

    if unmatched_products:
        print("\nПЪЛЕН СПИСЪК НА НЕНАМЕРЕНИТЕ ПРОДУКТИ:")
        for error in unmatched_products:
            print(error)

    print(f"\nОбновеният файл е запазен като: {output_path}")
    print("="*50)
