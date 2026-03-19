from database import DatabaseManager
from dotenv import load_dotenv

load_dotenv()

def seed():
    db = DatabaseManager()
    # Now simply calls the centralized seed method which uses base_ontology.json
    db.seed_ontology()
    print("Database seeded successfully using base_ontology.json")

if __name__ == "__main__":
    seed()
