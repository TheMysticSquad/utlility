import sqlite3

class DBManager:
    def __init__(self, db_name="consumer.db"):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self.init_db()

    def init_db(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS consumer (
            ca_number TEXT PRIMARY KEY,
            category TEXT,
            meter_reading INTEGER
        )
        """)
        self.conn.commit()

    def get_consumer(self, ca_number):
        self.cursor.execute("SELECT * FROM consumer WHERE ca_number=?", (ca_number,))
        return self.cursor.fetchone()

    def insert_or_update(self, ca_number, category, reading):
        self.cursor.execute("""
            REPLACE INTO consumer (ca_number, category, meter_reading)
            VALUES (?, ?, ?)
        """, (ca_number, category, reading))
        self.conn.commit()
