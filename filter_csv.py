import pandas as pd
import argparse
import sys

def filter_csv(input_path, output_path, search_string, mode, column_name):
    # Load CSV
    df = pd.read_csv(input_path)
    
    print(df.head())
    print(df.columns)
    if column_name not in df.columns:
        print(f"Error: Column '{column_name}' not found in CSV.")
        print(f"Available columns: {list(df.columns)}")
        sys.exit(1)

    # Convert column to string just in case
    df[column_name] = df[column_name].astype(str)

    # Case-insensitive match
    mask = df[column_name].str.contains(search_string, case=False, na=False)

    if mode == "remove":
        filtered_df = df[~mask]
    elif mode == "keep":
        filtered_df = df[mask]
    else:
        print("Mode must be 'remove' or 'keep'")
        sys.exit(1)

    # Save result
    filtered_df.to_csv(output_path, index=False)

    print(f"Done.")
    print(f"Original rows: {len(df)}")
    print(f"Filtered rows: {len(filtered_df)}")
    print(f"Saved to: {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Filter CSV by string in a column")

    parser.add_argument("--input", required=True, help="Input CSV file path")
    parser.add_argument("--output", default="filtered_output.csv", help="Output CSV file path")
    parser.add_argument("--search", required=True, help="String to search for")
    parser.add_argument("--mode", choices=["remove", "keep"], required=True, help="Filter mode")
    parser.add_argument("--column", default="SearchText", help="Column to search in (default: SearchText)")

    args = parser.parse_args()

    filter_csv(
        input_path=args.input,
        output_path=args.output,
        search_string=args.search,
        mode=args.mode,
        column_name=args.column
    )