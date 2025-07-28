from db_manager import Database
import pandas as pd


def initialize_and_seed_db():
    db = Database()
    db.connect()
    db.close_connection()
    print("Database initialized successfully.")


if __name__ == "__main__":
    initialize_and_seed_db()
