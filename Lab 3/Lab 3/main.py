import os
import json
import csv
from flatten_json import flatten

def flatten_json_file(json_file_path):
    """Flatten a JSON file and return the flattened data."""
    try:
        with open(json_file_path, 'r') as file_json:
            data = json.load(file_json)
            return flatten(data, separator='_')
    except Exception as e:
        print(f"Error processing file {json_file_path}: {e}")
        return None

def write_csv_from_json(flattened_json, csv_file_path):
    """Write a CSV file from flattened JSON data."""
    if not flattened_json:
        return

    try:
        with open(csv_file_path, 'w', newline='') as file_csv:
            csv_writer = csv.DictWriter(file_csv, fieldnames=flattened_json.keys())
            csv_writer.writeheader()
            csv_writer.writerow(flattened_json)
    except Exception as e:
        print(f"Error writing CSV file {csv_file_path}: {e}")

def process_json_directory_to_csv(json_folder_path, csv_folder_path):
    """Convert all JSON files in a directory to CSV files."""
    if not os.path.exists(csv_folder_path):
        os.makedirs(csv_folder_path)

    for root, dirs, files in os.walk(json_folder_path):
        for file in files:
            if file.lower().endswith('.json'):
                json_file_path = os.path.join(root, file)
                csv_file_path = os.path.join(csv_folder_path, os.path.splitext(file)[0] + '.csv')

                flattened_json = flatten_json_file(json_file_path)
                write_csv_from_json(flattened_json, csv_file_path)

def main(json_folder='./data', csv_folder='./csv_output'):
    """Main function to convert JSON files in a folder to CSV format."""
    process_json_directory_to_csv(json_folder, csv_folder)

if __name__ == "__main__":
    main()
