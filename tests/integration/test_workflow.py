import pandas as pd
import pytest
from pathlib import Path
import os

class TestIntegrationWorkflow:
    @pytest.fixture
    def sample_products_file(self, tmp_path):
        """Create a sample products Excel file for integration testing"""
        file_path = tmp_path / "test_products.xlsx"
        df = pd.DataFrame({
            'Title': ['Product 1', 'Product 2'],
            'Handle': ['product-1', 'product-2'],
            'Variant SKU': ['SKU1', 'SKU2'],
            'Type': ['', ''],
            'Metafield: woo.woobt_ids': ['', ''],
            'Metafield: global.Combined handle': ['', ''],
            'Metafield: woo.id': ['1', '2'],
            'Variant Metafield: woo.id': ['v1', 'v2'],
            'Metafield: woo.xts-blocks-test': ['value1', 'value2']  # Should be removed
        })
        df.to_excel(file_path, sheet_name='Products', index=False)
        return file_path
    
    def test_full_workflow(self, sample_products_file, tmp_path):
        """Test the full workflow from input to output"""
        from processing.pipeline import process_woocommerce_to_shopify
        
        # Arrange
        output_file = tmp_path / "output.xlsx"
        
        # Act
        result = process_woocommerce_to_shopify(
            str(sample_products_file),
            str(output_file)
        )
        
        # Assert
        assert result == str(output_file)
        assert os.path.exists(result)
        
        # Verify the output file
        result_df = pd.read_excel(result, sheet_name='Products')
        
        # Check that xts-blocks column was removed
        assert 'Metafield: woo.xts-blocks-test' not in result_df.columns
        
        # Check that required columns are present
        required_columns = [
            'Title', 'Handle', 'Variant SKU', 'Type',
            'Metafield: woo.woobt_ids', 'Metafield: global.Combined handle',
            'Metafield: woo.id', 'Variant Metafield: woo.id'
        ]
        for col in required_columns:
            assert col in result_df.columns
