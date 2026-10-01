from pymongo import MongoClient
import urllib.parse

username = urllib.parse.quote_plus("ShibangiPipeline")
password = urllib.parse.quote_plus("Test12345")
cluster_host = "clustershibangi.cpuw87u.mongodb.net"

# Construct URI with explicit URL-encoded credentials and authSource
uri = f"mongodb+srv://{username}:{password}@{cluster_host}/quick_commerce_db?retryWrites=true&w=majority&authSource=admin"

print("--- TESTING MONGODB ATLAS CONNECTION ---")
print(f"Connecting to host: {cluster_host}...")

try:
    # Explicitly set client parameters
    client = MongoClient(
        uri,
        authSource="admin",
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000
    )
    
    # Ping the admin database to verify authentication
    response = client.admin.command('ping')
    print("✅ SUCCESS: Ping response from MongoDB Atlas:", response)
    
    # Verify collection access
    db = client["quick_commerce_db"]
    collections = db.list_collection_names()
    print("✅ SUCCESS: Connected to database 'quick_commerce_db'. Existing collections:", collections)

except Exception as e:
    print("\n❌ CONNECTION FAILED:")
    print(e)