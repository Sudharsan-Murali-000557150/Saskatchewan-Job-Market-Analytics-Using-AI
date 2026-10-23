import os, json
from pymongo import MongoClient
from bson import json_util

# Uses the same MONGODB_URI as the pipeline (from .env or your environment)
try:
    from dotenv import load_dotenv; load_dotenv()
except ImportError:
    pass

uri = os.environ["MONGODB_URI"]
client = MongoClient(uri, serverSelectionTimeoutMS=20000)
client.admin.command("ping")

for db_name in client.list_database_names():
    if db_name in ("admin", "local", "config"):
        continue
    db = client[db_name]
    for coll in db.list_collection_names():
        docs = list(db[coll].find())
        out = f"{db_name}.{coll}.json"
        with open(out, "w", encoding="utf-8") as f:
            f.write(json_util.dumps(docs, indent=1))
        print(f"{out}: {len(docs)} documents")