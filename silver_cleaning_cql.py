
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json

# === Connect to Cassandra ===
cloud_config = {
    'secure_connect_bundle': 'secure-connect-ecommerce-db-cql.zip'
}

with open('ecommerce_db_cql-token.json') as f:
    secrets = json.load(f)

auth_provider = PlainTextAuthProvider(secrets["clientId"], secrets["secret"])
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()
session.set_keyspace('ecommerce')

# === Create the Silver Table ===
session.execute("""
    CREATE TABLE IF NOT EXISTS silver_clean_orders (
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
print("✅ Silver table created.")

# === Filter clean rows from Bronze and insert into Silver ===
rows = session.execute("""
    SELECT * FROM bronze_raw_orders
    WHERE quantity > 0 AND unit_price > 0 ALLOW FILTERING
""")

inserted = 0
for row in rows:
    if row.customer_id and row.customer_id.strip():
        try:
            session.execute("""
                INSERT INTO silver_clean_orders (
                    invoice_no, stock_code, description, quantity,
                    invoice_date, unit_price, customer_id, country
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row.invoice_no, row.stock_code, row.description,
                row.quantity, row.invoice_date, row.unit_price,
                row.customer_id, row.country
            ))
            inserted += 1
        except:
            continue

print(f"✅ Inserted {inserted} clean rows into silver_clean_orders.")
count = session.execute("SELECT COUNT(*) FROM silver_clean_orders").one()[0]
print(f"📊 Total rows in silver_clean_orders: {count}")
