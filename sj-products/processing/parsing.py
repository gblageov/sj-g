import json
import ast
import re
from typing import Any, Dict, Optional, Tuple


_JSON_BLOCK_REGEX = re.compile(r'^[^{]*({.*})[^}]*$')


def normalize_woobt_string(raw: Any) -> str:
    """
    Extracts the JSON-like object from a noisy string using regex, mirroring the original behavior.
    Returns the cleaned string (may still be invalid JSON).
    """
    s = '' if raw is None else str(raw)
    match = _JSON_BLOCK_REGEX.sub(r'\1', s)
    return match


def extract_woobt_dict(raw: Any) -> Tuple[Optional[Dict[str, dict]], Optional[str]]:
    """
    Tries to parse the woobt_ids cell content into a dict.
    Returns (data_dict, error_message). If parsing fails, data_dict is None and error_message is set.
    Only returns a dict if the parsed value is indeed a dict.
    """
    cleaned = normalize_woobt_string(raw)

    # First try JSON
    try:
        data = json.loads(cleaned)
        if isinstance(data, dict):
            return data, None
        return None, None
    except Exception:
        pass

    # Fallback to Python literal eval
    try:
        data = ast.literal_eval(cleaned)
        if isinstance(data, dict):
            return data, None
        return None, None
    except Exception:
        return None, f"Неуспешно разчитане на JSON -> '{cleaned}'"
