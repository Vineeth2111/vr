from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json

# Zip bundle path (zipped!)
cloud_config = {
    'secure_connect_bundle': 'secure-connect-ecommerce-db-cql.zip'
}

# Load credentials from token
with open('ecommerce_db_cql_token.json') as f:
    secrets = json.load(f)

CLIENT_ID = secrets["clientId"]
CLIENT_SECRET = secrets["secret"]

auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

row = session.execute("SELECT release_version FROM system.local").one()
print("✅ Connected to Cassandra version:", row[0])
