from typing import List, Optional
import pandas as pd


def infer_type_from_title(title: str, product_types: List[str]) -> Optional[str]:
    """
    Returns the longest product type that matches the beginning of the title (case-insensitive).
    If no match is found or title is falsy, returns None.
    """
    if not title:
        return None

    title_lower = str(title).strip().lower()
    if not title_lower:
        return None

    found_matches = []
    for product_type in product_types:
        if title_lower.startswith(product_type.lower()):
            found_matches.append(product_type)

    if not found_matches:
        return None

    return max(found_matches, key=len)


def populate_type_column(df: pd.DataFrame, product_types: List[str]) -> int:
    """
    Populates df['Type'] with the best match from product_types based on the Title.
    Returns the number of rows where a type was added (matches original behavior's counter).
    """
    types_added_count = 0

    for idx, row in df.iterrows():
        title = str(row.get('Title', '')).strip()
        if not title:
            continue

        best_match = infer_type_from_title(title, product_types)
        if best_match:
            df.at[idx, 'Type'] = best_match
            types_added_count += 1

    return types_added_count
