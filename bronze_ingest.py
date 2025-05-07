import pandas as pd
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json

# -- Connect to Astra DB --
cloud_config = {
    'secure_connect_bundle': 'secure-connect-ecommerce-db-cql.zip'
}

with open('ecommerce_db_cql-token.json') as f:
    secrets = json.load(f)

auth_provider = PlainTextAuthProvider(secrets['clientId'], secrets['secret'])
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()
session.set_keyspace('ecommerce')

# -- Create the Bronze Table --
session.execute("""
    CREATE TABLE IF NOT EXISTS bronze_raw_orders (
        invoice_no text,
        stock_code text,
        description text,
        quantity int,
        invoice_date text,
        unit_price float,
        customer_id text,
        country text,
        PRIMARY KEY (invoice_no, stock_code)
    )
""")
print("✅ Bronze table created.")

# -- Read the full CSV --
df = pd.read_csv("ecommerce_data.csv", encoding='ISO-8859-1').fillna("")

# -- Insert all rows into Bronze table --
inserted = 0
for _, row in df.iterrows():
    try:
        session.execute("""
            INSERT INTO bronze_raw_orders (
                invoice_no, stock_code, description, quantity,
                invoice_date, unit_price, customer_id, country
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            str(row['InvoiceNo']), str(row['StockCode']), str(row['Description']),
            int(row['Quantity']), str(row['InvoiceDate']),
            float(row['UnitPrice']), str(row['CustomerID']), str(row['Country'])
        ))
        inserted += 1
    except Exception as e:
        continue

print(f"✅ Inserted {inserted} rows into bronze_raw_orders.")
count = session.execute("SELECT COUNT(*) FROM bronze_raw_orders").one()[0]
print(f"📊 Total rows in bronze_raw_orders: {count}")


