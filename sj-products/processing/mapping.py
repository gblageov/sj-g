from typing import Dict
import pandas as pd


def build_sku_to_handle(df: pd.DataFrame) -> Dict[str, str]:
    """
    Builds a mapping from Variant SKU -> last non-empty Handle encountered while iterating rows.
    Mirrors the original logic using a rolling last_valid_handle.
    """
    sku_to_handle: Dict[str, str] = {}
    last_valid_handle = ''

    for _, row in df.iterrows():
        handle = row.get('Handle')
        if pd.notna(handle) and str(handle).strip() != '':
            last_valid_handle = str(handle).strip()

        if not last_valid_handle:
            continue

        variant_sku = row.get('Variant SKU')
        if pd.notna(variant_sku) and str(variant_sku).strip() != '':
            sku_to_handle[str(variant_sku).strip()] = last_valid_handle

    return sku_to_handle


def build_woo_id_to_handle(df: pd.DataFrame) -> Dict[str, str]:
    """
    Builds a mapping from Woo ID (stringified int) -> last non-empty Handle encountered.
    Prefers 'Metafield: woo.id' if present, else uses 'Variant Metafield: woo.id'.
    """
    woo_id_to_handle: Dict[str, str] = {}
    last_valid_handle = ''

    for _, row in df.iterrows():
        handle = row.get('Handle')
        if pd.notna(handle) and str(handle).strip() != '':
            last_valid_handle = str(handle).strip()

        if not last_valid_handle:
            continue

        raw_id = row.get('Metafield: woo.id')
        if not (pd.notna(raw_id) and str(raw_id).strip() != ''):
            raw_id = row.get('Variant Metafield: woo.id')

        if pd.notna(raw_id) and str(raw_id).strip() != '':
            try:
                norm_id = str(int(float(raw_id)))
                woo_id_to_handle[norm_id] = last_valid_handle
            except (ValueError, TypeError):
                continue

    return woo_id_to_handle
