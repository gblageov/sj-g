# import unittest
# import pandas as pd
# import os
# # Импортираме функцията, която искаме да тестваме
# from process_products import process_woocommerce_to_shopify, TARGET_METAFIELD_COLUMN

# class TestWooCommerceProcessing(unittest.TestCase):

#     def setUp(self):
#         """Тази функция се изпълнява преди всеки тест."""
#         # Създаваме тестови данни в паметта
#         self.test_data = {
#             'Handle': ['product-a', 'product-b', 'product-c', 'product-d-no-ids', 'product-e-bad-json'],
#             'Variant SKU': ['', 'SKU-B', '', '', ''],
#             'Metafield: woo.id': [101, 102, 103, 104, 105],
#             'Variant Metafield: woo.id': ['', '', '', '', ''],
#             'Metafield: woo.woobt_ids': [
#                 '{"1":{"id":102,"sku":"SKU-B","qty":1}}', # Трябва да намери product-b
#                 '', # Няма данни за обработка
#                 '{"1":{"id":999,"sku":"non-existent"}, "2":{"id":101, "sku":""}}', # Трябва да намери само product-a
#                 '', # Няма данни
#                 '{"invalid json structure', # Грешен JSON
#             ]
#         }
#         self.df = pd.DataFrame(self.test_data)
        
#         # Записваме тестовите данни във временен Excel файл
#         self.test_input_file = 'temp_test_input.xlsx'
#         self.test_output_file = 'temp_test_input_updated.xlsx'
#         self.df.to_excel(self.test_input_file, index=False, sheet_name='Products', engine='xlsxwriter')

#     def tearDown(self):
#         """Тази функция се изпълнява след всеки тест, за да почисти."""
#         if os.path.exists(self.test_input_file):
#             os.remove(self.test_input_file)
#         if os.path.exists(self.test_output_file):
#             os.remove(self.test_output_file)

#     def test_column_creation_and_position(self):
#         """Тест 1: Проверява дали новата колона е създадена на правилното място."""
#         process_woocommerce_to_shopify(self.test_input_file)
        
#         # Проверяваме дали изходният файл е създаден
#         self.assertTrue(os.path.exists(self.test_output_file))
        
#         # Зареждаме резултата
#         result_df = pd.read_excel(self.test_output_file, engine='openpyxl')
        
#         # Проверка 1: Дали колоната съществува
#         self.assertIn(TARGET_METAFIELD_COLUMN, result_df.columns)
        
#         # Проверка 2: Дали е на правилната позиция
#         columns = list(result_df.columns)
#         target_col_index = columns.index(TARGET_METAFIELD_COLUMN)
#         reference_col_index = columns.index('Metafield: woo.woobt_ids')
        
#         self.assertEqual(target_col_index, reference_col_index - 1)

#     def test_handle_mapping_logic(self):
#         """Тест 2: Проверява дали данните в новата колона са коректни."""
#         process_woocommerce_to_shopify(self.test_input_file)
#         result_df = pd.read_excel(self.test_output_file, engine='openpyxl')
        
#         # Ред 1 (product-a) трябва да съдържа handle-а на продукт с ID 102 -> 'product-b'
#         self.assertEqual(result_df.loc[0, TARGET_METAFIELD_COLUMN], 'product-b')

#         # Ред 3 (product-c) трябва да съдържа handle-а на продукт с ID 101 -> 'product-a'
#         self.assertEqual(result_df.loc[2, TARGET_METAFIELD_COLUMN], 'product-a')
        
#         # Ред 5 (product-e-bad-json) трябва да има празна стойност, защото JSON е грешен
#         self.assertTrue(pd.isna(result_df.loc[4, TARGET_METAFIELD_COLUMN]) or result_df.loc[4, TARGET_METAFIELD_COLUMN] == '')

# # Това позволява да стартираме тестовете директно от терминала
# if __name__ == '__main__':
#     unittest.main()