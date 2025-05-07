from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
import matplotlib.pyplot as plt

# Connect to Astra
cloud_config = {
    'secure_connect_bundle': 'secure-connect-ecommerce-db-cql.zip'
}

with open('ecommerce_db_cql-token.json') as f:
    secrets = json.load(f)

auth_provider = PlainTextAuthProvider(secrets["clientId"], secrets["secret"])
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()
session.set_keyspace('ecommerce')

# Fetch product sales
rows = session.execute("SELECT * FROM gold_top_products")

products = []
sales = []

for row in rows:
    products.append(row.description)
    sales.append(row.total_sales)

# Get Top 10 products
top_10 = sorted(zip(products, sales), key=lambda x: x[1], reverse=True)[:10]
top_products, top_sales = zip(*top_10)

# Plot
plt.figure(figsize=(12, 6))
plt.bar(top_products, top_sales, color='orange')
plt.xticks(rotation=45, ha='right')
plt.xlabel("Product")
plt.ylabel("Total Sales")
plt.title("Top 10 Best-Selling Products")
plt.tight_layout()
plt.grid(axis='y', linestyle='--', alpha=0.5)
plt.show()
