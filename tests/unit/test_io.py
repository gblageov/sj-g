import pandas as pd
import pytest
from processing.io import read_products_df, remove_xts_blocks_columns
from pathlib import Path
import os

class TestRemoveXTSBlocksColumns:
    def test_remove_columns_with_xts_blocks(self):
        """Test removing columns containing 'Metafield: woo.xts-blocks' in their names"""
        # Arrange
        data = {
            'Name': ['Product 1', 'Product 2'],
            'Metafield: woo.xts-blocks-test': [1, 2],
            'Other Field': ['A', 'B']
        }
        df = pd.DataFrame(data)
        
        # Act
        result_df, removed_count = remove_xts_blocks_columns(df)
        
        # Assert
        assert removed_count == 1
        assert 'Metafield: woo.xts-blocks-test' not in result_df.columns
        assert len(result_df.columns) == 2  # Should only have 'Name' and 'Other Field' left
        
    def test_no_columns_to_remove(self):
        """Test when no columns contain 'Metafield: woo.xts-blocks'"""
        # Arrange
        data = {
            'Name': ['Product 1', 'Product 2'],
            'Other Field': ['A', 'B']
        }
        df = pd.DataFrame(data)
        
        # Act
        result_df, removed_count = remove_xts_blocks_columns(df)
        
        # Assert
        assert removed_count == 0
        assert len(result_df.columns) == 2
        
    def test_empty_dataframe(self):
        """Test with an empty DataFrame"""
        # Arrange
        df = pd.DataFrame()
        
        # Act
        result_df, removed_count = remove_xts_blocks_columns(df)
        
        # Assert
        assert removed_count == 0
        assert result_df.empty

class TestReadProductsDF:
    @pytest.fixture
    def sample_excel_file(self, tmp_path):
        """Create a sample Excel file for testing"""
        file_path = tmp_path / "test_products.xlsx"
        df = pd.DataFrame({
            'Title': ['Product 1', 'Product 2'],
            'Handle': ['product-1', 'product-2'],
            'Variant SKU': ['SKU1', 'SKU2'],
            'Metafield: woo.woobt_ids': ['', ''],
            'Metafield: global.Combined handle': ['', ''],
            'Metafield: woo.id': ['1', '2'],
            'Variant Metafield: woo.id': ['v1', 'v2']
        })
        df.to_excel(file_path, sheet_name='Products', index=False)
        return file_path
    
    def test_read_valid_file(self, sample_excel_file):
        """Test reading a valid Excel file"""
        # Act
        result = read_products_df(str(sample_excel_file))
        
        # Assert
        assert result is not None
        assert 'Title' in result.columns
        assert len(result) == 2
        
    def test_file_not_found(self):
        """Test handling of non-existent file"""
        # Act
        result = read_products_df("non_existent_file.xlsx")
        
        # Assert
        assert result is None
