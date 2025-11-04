import argparse
import sys
import os

# Ensure we can import the local 'processing' package when running directly
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from processing.pipeline import process_woocommerce_to_shopify

def main():
    parser = argparse.ArgumentParser(description="Process WooCommerce to Shopify Excel export.")
    parser.add_argument(
        "-i", "--input",
        default="import_result.xlsx",
        help="Path to input Excel file (default: import_result.xlsx)",
    )
    args = parser.parse_args()

    output = process_woocommerce_to_shopify(args.input)
    if output:
        print(output)
        sys.exit(0)
    sys.exit(1)

if __name__ == "__main__":
    main()
