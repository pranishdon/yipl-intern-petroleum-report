import requests
import sqlite3

# Step 1: Fetch Data from the API
api_url = "https://raw.githubusercontent.com/younginnovations/internship-challenges/master/programming/petroleum-report/data.json"
response = requests.get(api_url)

if response.status_code == 200:
    data = response.json()
else:
    print("Failed to fetch data from the API.")
    exit()

# Step 2: Store Data in SQLite Database
database_file = "petroleum_data.db"

try:
    # Connect to the SQLite database (it will be created if it doesn't exist)
    conn = sqlite3.connect(database_file)
    cursor = conn.cursor()

    # Create tables for normalized data
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS countries (
            id INTEGER PRIMARY KEY,
            name TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY,
            name TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sales (
            id INTEGER PRIMARY KEY,
            country_id INTEGER,
            product_id INTEGER,
            year INTEGER,
            sale REAL
        )
    ''')

    # Define dictionaries to store unique countries and products
    unique_countries = {}
    unique_products = {}

    for entry in data:
        # Normalize the data: Add countries and products to dictionaries if not already present
        country_name = entry['country']
        product_name = entry['petroleum_product']

        country_id = unique_countries.setdefault(country_name, len(unique_countries) + 1)
        product_id = unique_products.setdefault(product_name, len(unique_products) + 1)

        # Insert data into the normalized tables
        cursor.execute("INSERT OR IGNORE INTO countries (name) VALUES (?)", (country_name,))
        cursor.execute("INSERT OR IGNORE INTO products (name) VALUES (?)", (product_name,))
        cursor.execute("INSERT INTO sales (country_id, product_id, year, sale) VALUES (?, ?, ?, ?)",
                       (country_id, product_id, entry['year'], entry['sale']))

    # Commit the changes to the database
    conn.commit()
    print("Data successfully stored in the SQLite database.")

except sqlite3.Error as e:
    print("SQLite Error:", e)
    exit()

finally:
    # Close the database connection
    conn.close()
