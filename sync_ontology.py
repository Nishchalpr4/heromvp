from database import DatabaseManager
import logging

logging.basicConfig(level=logging.INFO)

def sync():
    db = DatabaseManager()
    print("Syncing ontology from base_ontology.json to Neon DB...")
    # merge_with_existing=False ensures we OVERWRITE with the clean JSON rules
    db.seed_ontology(merge_with_existing=False)
    print("Sync complete.")

if __name__ == "__main__":
    sync()
