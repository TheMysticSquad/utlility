import psycopg2
import random
from datetime import date, timedelta
from faker import Faker
import os
from dotenv import load_dotenv
load_dotenv()

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'sslmode': os.getenv('DB_SSLMODE', 'require')
}

fake = Faker()
connection_types = ['Domestic', 'NDS', 'LTIS', 'IAS', 'HTS']
reading_parameters_map = {
    'Domestic': ['kWh'],
    'NDS': ['kWh'],
    'LTIS': ['kVAh', 'maximum_demand', 'power_factor'],
    'IAS': ['kWh', 'contract_demand'],
    'HTS': ['kVAh', 'maximum_demand', 'contract_demand']
}

def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def create_tables():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS consumers (
                    id SERIAL PRIMARY KEY,
                    meter_number VARCHAR(50) UNIQUE NOT NULL,
                    consumer_name VARCHAR(100),
                    connection_type VARCHAR(50) NOT NULL,
                    contract_demand DECIMAL(10,2),
                    connected_load DECIMAL(10,2),
                    arrear_amount DECIMAL(10,2) DEFAULT 0.0
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS reading_parameters (
                    id SERIAL PRIMARY KEY,
                    connection_type VARCHAR(50) NOT NULL,
                    parameter_name VARCHAR(50) NOT NULL
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS meter_readings (
                    id SERIAL PRIMARY KEY,
                    consumer_id INT REFERENCES consumers(id),
                    reading_date DATE NOT NULL,
                    parameter_name VARCHAR(50) NOT NULL,
                    reading_value DECIMAL(12, 2) NOT NULL
                );
            """)
        conn.commit()

def insert_reading_parameters():
    with connect_db() as conn:
        with conn.cursor() as cur:
            for connection_type, parameters in reading_parameters_map.items():
                for param in parameters:
                    cur.execute("""
                        INSERT INTO reading_parameters (connection_type, parameter_name)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING;
                    """, (connection_type, param))
        conn.commit()

def insert_random_consumers(n=30):
    with connect_db() as conn:
        with conn.cursor() as cur:
            for _ in range(n):
                name = fake.name()
                meter_number = fake.unique.numerify(text="##########")
                connection_type = random.choice(connection_types)
                contract_demand = round(random.uniform(5, 50), 2)
                connected_load = round(random.uniform(1, 20), 2)
                arrear_amount = round(random.uniform(0, 1000), 2)
                cur.execute("""
                    INSERT INTO consumers (meter_number, consumer_name, connection_type, contract_demand, connected_load, arrear_amount)
                    VALUES (%s, %s, %s, %s, %s, %s);
                """, (meter_number, name, connection_type, contract_demand, connected_load, arrear_amount))
        conn.commit()

def update_random_readings(n=10):
    today = date.today()
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, connection_type FROM consumers ORDER BY RANDOM() LIMIT %s;", (n,))
            consumers = cur.fetchall()
            for consumer_id, connection_type in consumers:
                parameters = reading_parameters_map[connection_type]
                for param in parameters:
                    reading_value = round(random.uniform(100, 10000), 2)
                    cur.execute("""
                        INSERT INTO meter_readings (consumer_id, reading_date, parameter_name, reading_value)
                        VALUES (%s, %s, %s, %s);
                    """, (consumer_id, today, param, reading_value))
                # Randomly update arrear
                if random.choice([True, False]):
                    new_arrear = round(random.uniform(0, 1500), 2)
                    cur.execute("UPDATE consumers SET arrear_amount = %s WHERE id = %s;", (new_arrear, consumer_id))
        conn.commit()

# Run all actions in order
create_tables()
insert_reading_parameters()
insert_random_consumers()
update_random_readings()

"Database schema created, and demo data inserted to simulate daily activity."
