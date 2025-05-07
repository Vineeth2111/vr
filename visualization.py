from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
import matplotlib.pyplot as plt

# === Connect to Cassandra ===
cloud_config = {
    'secure_connect_bundle': 'secure-connect-ecommerce-db-cql.zip'
}

with open('ecommerce_db_cql_token.json') as f:
    secrets = json.load(f)

auth_provider = PlainTextAuthProvider(secrets["clientId"], secrets["secret"])
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()
session.set_keyspace('ecommerce')

# === Fetch data from gold_sales_by_country ===
rows = session.execute("SELECT * FROM gold_sales_by_country")

countries = []
sales = []

for row in rows:
    countries.append(row.country)
    sales.append(row.total_sales)

# === 1. Vertical Bar Chart ===
plt.figure(figsize=(10, 6))
plt.bar(countries, sales, color='skyblue')
plt.title("Total Sales by Country (Vertical Bar)")
plt.xlabel("Country")
plt.ylabel("Total Sales")
plt.xticks(rotation=45)
plt.tight_layout()
plt.grid(axis='y', linestyle='--', alpha=0.5)
plt.show()

# === 2. Pie Chart (Top 5 Countries) ===
top_5 = sorted(zip(countries, sales), key=lambda x: x[1], reverse=True)[:5]
labels, values = zip(*top_5)

plt.figure(figsize=(6, 6))
plt.pie(values, labels=labels, autopct='%1.1f%%', startangle=140)
plt.title("Top 5 Countries by Sales")
plt.tight_layout()
plt.show()

# === 3. Horizontal Bar Chart ===
plt.figure(figsize=(10, 6))
plt.barh(countries, sales, color='green')
plt.title("Sales by Country (Horizontal Bar)")
plt.xlabel("Total Sales")
plt.ylabel("Country")
plt.tight_layout()
plt.grid(axis='x', linestyle='--', alpha=0.5)
plt.show()
