#!/usr/bin/env python3
"""
Shopify Order Data Validator
Main entry point for the order data validation and fixing application.
"""

import os
import sys

# Add the current directory to the Python path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from gui import main

if __name__ == "__main__":
    main()
