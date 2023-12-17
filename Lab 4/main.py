import psycopg2
import os
import csv

def get_db_config():
    """ Returns the database configuration. """
    return {
        "host": "postgres",
        "database": "postgres",
        "user": "postgres",
        "password": "postgres"
    }

def open_database_connection():
    """ Opens a database connection with the configuration settings. """
    config = get_db_config()
    return psycopg2.connect(**config)

def read_csv_header_and_sample_row(csv_path):
    """ Reads the header and a sample row from a CSV file. """
    with open(csv_path, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        header = next(csv_reader)
        sample_row = next(csv_reader)
    return header, sample_row

def determine_data_types(sample_row):
    """ Determines data types for SQL table creation based on a sample row. """
    data_types = []
    for value in sample_row:
        if value.isdigit():
            data_types.append("INTEGER")
        elif value.replace(".", "", 1).isdigit():
            data_types.append("REAL")
        else:
            data_types.append("VARCHAR(255)")
    return data_types

def create_table(conn, cursor, table_name, csv_path):
    """ Creates a table in the database based on the structure of the CSV file. """
    header, sample_row = read_csv_header_and_sample_row(csv_path)
    data_types = determine_data_types(sample_row)

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {", ".join([f"{field} {data_type}" for field, data_type in zip(header, data_types)])}
        );
    """)
    conn.commit()
    print(f"Table {table_name} created successfully.")

def insert_data_from_csv(conn, cursor, table_name, csv_path):
    """ Inserts data into a table from a CSV file. """
    with open(csv_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            placeholders = ', '.join(['%s'] * len(row))
            columns = ', '.join(row.keys())
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, list(row.values()))
    conn.commit()

def display_table_info(cursor, table_name):
    """ Displays information about a table. """
    cursor.execute(f"SELECT * FROM {table_name};")
    print(f"\nTable: {table_name}\nRows:")
    for row in cursor.fetchall():
        print(row)

def main():
    with open_database_connection() as conn:
        cursor = conn.cursor()

        # Define CSV paths
        csv_files = {
            "accounts": os.path.join("data", "accounts.csv"),
            "products": os.path.join("data", "products.csv"),
            "transactions": os.path.join("data", "transactions.csv")
        }

        # Create tables and insert data
        for table_name, csv_path in csv_files.items():
            create_table(conn, cursor, table_name, csv_path)
            insert_data_from_csv(conn, cursor, table_name, csv_path)

        # Display table information
        for table_name in csv_files.keys():
            display_table_info(cursor, table_name)

        cursor.close()

if __name__ == "__main__":
    main()
