from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
from collections import defaultdict

# === Connect to Cassandra ===
cloud_config = {
    'secure_connect_bundle': 'secure-connect-ecommerce-db-cql.zip'
}

with open('ecommerce_db_cql-token.json') as f:
    secrets = json.load(f)

auth_provider = PlainTextAuthProvider(secrets['clientId'], secrets['secret'])
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()
session.set_keyspace('ecommerce')

# === Create Gold table for country-wise aggregation ===
session.execute("""
    CREATE TABLE IF NOT EXISTS gold_sales_by_country (
        country text PRIMARY KEY,
        total_sales double
    )
""")
print("✅ Gold table (sales by country) created.")

# === Read clean data and aggregate sales per country ===
rows = session.execute("SELECT * FROM silver_clean_orders")
sales_map = defaultdict(float)

for row in rows:
    try:
        total = float(row.quantity) * float(row.unit_price)
        if row.country:
            sales_map[row.country] += total
    except:
        continue

# === Insert aggregated data into gold table ===
inserted = 0
for country, total_sales in sales_map.items():
    try:
        session.execute("""
            INSERT INTO gold_sales_by_country (country, total_sales)
            VALUES (%s, %s)
        """, (country, total_sales))
        inserted += 1
    except:
        continue

print(f"✅ Inserted {inserted} rows into gold_sales_by_country.")
count = session.execute("SELECT COUNT(*) FROM gold_sales_by_country").one()[0]
print(f"📊 Total rows in gold_sales_by_country: {count}")