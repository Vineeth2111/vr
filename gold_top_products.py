from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
from collections import defaultdict

# Connect to Astra DB
cloud_config = {
    'secure_connect_bundle': 'secure-connect-ecommerce-db-cql.zip'
}

with open('ecommerce_db_cql-token.json') as f:
    secrets = json.load(f)

auth_provider = PlainTextAuthProvider(secrets["clientId"], secrets["secret"])
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()
session.set_keyspace('ecommerce')

# Create Gold table for top-selling products
session.execute("""
    CREATE TABLE IF NOT EXISTS gold_top_products (
        description text PRIMARY KEY,
        total_sales double
    )
""")
print("✅ Gold table (top-selling products) created.")

# Aggregate sales from silver table
rows = session.execute("SELECT * FROM silver_clean_orders")
product_sales = defaultdict(float)

for row in rows:
    try:
        total = float(row.quantity) * float(row.unit_price)
        if row.description:
            product_sales[row.description] += total
    except:
        continue

# Insert into gold_top_products
inserted = 0
for desc, total in product_sales.items():
    try:
        session.execute("""
            INSERT INTO gold_top_products (description, total_sales)
            VALUES (%s, %s)
        """, (desc, total))
        inserted += 1
    except:
        continue

print(f"✅ Inserted {inserted} rows into gold_top_products.")
count = session.execute("SELECT COUNT(*) FROM gold_top_products").one()[0]
print(f"📊 Total rows in gold_top_products: {count}")
