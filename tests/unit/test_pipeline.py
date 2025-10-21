import pytest
import pandas as pd
from pathlib import Path
import os
from unittest.mock import patch, MagicMock

# Import the function to test
from processing.pipeline import process_woocommerce_to_shopify

class TestProcessWooCommerceToShopify:
    @pytest.fixture
    def sample_data(self):
        """Create sample data for testing"""
        return {
            'Title': ['Product 1', 'Product 2'],
            'Handle': ['product-1', 'product-2'],
            'Variant SKU': ['SKU1', 'SKU2'],
            'Type': ['', ''],
            'Metafield: woo.woobt_ids': ['', ''],
            'Metafield: global.Combined handle': ['', ''],
            'Metafield: woo.id': ['1', '2'],
            'Variant Metafield: woo.id': ['v1', 'v2']
        }

    @patch('processing.pipeline.io_mod.read_products_df')
    @patch('processing.pipeline.populate_type_column')
    @patch('processing.pipeline.build_sku_to_handle')
    @patch('processing.pipeline.build_woo_id_to_handle')
    @patch('processing.pipeline.io_mod.write_products_df')
    def test_process_woocommerce_to_shopify(
        self, 
        mock_write_df, 
        mock_build_woo_id, 
        mock_build_sku,
        mock_populate_type,
        mock_read_df,
        sample_data,
        tmp_path
    ):
        # Arrange
        # Setup mocks
        mock_df = pd.DataFrame(sample_data)
        mock_read_df.return_value = mock_df
        mock_populate_type.return_value = 2  # 2 types added
        mock_build_sku.return_value = {'SKU1': 'product-1', 'SKU2': 'product-2'}
        mock_build_woo_id.return_value = {'1': 'product-1', '2': 'product-2'}
        
        output_file = tmp_path / "output.xlsx"
        
        # Act
        result = process_woocommerce_to_shopify("dummy_input.xlsx", str(output_file))
        
        # Assert
        assert result == str(output_file)
        mock_read_df.assert_called_once_with("dummy_input.xlsx")
        mock_write_df.assert_called_once()
        assert mock_populate_type.call_count == 1
        assert mock_build_sku.call_count == 1
        assert mock_build_woo_id.call_count == 1

    @patch('processing.pipeline.io_mod.read_products_df')
    def test_process_woocommerce_to_shopify_file_not_found(self, mock_read_df):
        # Arrange
        mock_read_df.return_value = None
        
        # Act
        result = process_woocommerce_to_shopify("nonexistent.xlsx")
        
        # Assert
        assert result is None
        mock_read_df.assert_called_once_with("nonexistent.xlsx")
