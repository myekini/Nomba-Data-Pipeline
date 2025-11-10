"""
Nomba Data Engineer Assessment - Sample Data Generator
-------------------------------------------------------
Generates realistic test data for MongoDB (users)
and PostgreSQL (savings transactions and plans).
Supports incremental updates for CDC simulation.
"""

import random
import uuid
from datetime import datetime, timedelta
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_values
from faker import Faker
import argparse
import sys
import os


# CONFIGURATION

# MongoDB Atlas connection
MONGO_URI = (
    "mongodb+srv://myekini:muhammadyk@"
    "nomba-cluster.mtjrdmh.mongodb.net/?retryWrites=true&w=majority&appName=nomba-cluster"
)

# PostgreSQL (Aiven) connection
POSTGRES_SOURCE_URI = (
    f"host={os.getenv('PG_HOST')} "
    f"port={os.getenv('PG_PORT')} "
    f"user={os.getenv('PG_USER')} "
    f"password={os.getenv('PG_PASSWORD')} "
    f"dbname={os.getenv('PG_DB')} "
    f"sslmode={os.getenv('PG_SSLMODE', 'require')}"
)


fake = Faker()
Faker.seed(42)
random.seed(42)

# Nigerian-specific data
NIGERIAN_STATES = [
    "Lagos", "Kano", "Rivers", "Kaduna", "Oyo", "Abuja", "Anambra", "Edo",
    "Delta", "Ogun", "Imo", "Enugu", "Akwa Ibom", "Benue", "Plateau", "Osun",
    "Cross River", "Bayelsa", "Ondo", "Kwara"
]

OCCUPATIONS = [
    "Software Engineer", "Trader", "Teacher", "Doctor", "Accountant", "Nurse",
    "Banker", "Entrepreneur", "Student", "Civil Servant", "Lawyer", "Architect",
    "Pharmacist", "Engineer", "Marketing", "Sales", "Driver", "Artisan", "Farmer"
]

PRODUCT_TYPES = ["fixed_savings", "target_savings", "flexible_savings", "naira_savings", "dollar_savings"]
FREQUENCIES = ["daily", "weekly", "monthly"]
STATUSES = ["active", "completed", "paused", "cancelled"]
TRANSACTION_SIDES = ["buy", "sell"]
CURRENCIES = ["NGN", "USD", "GBP", "EUR"]


# DATA GENERATION FUNCTIONS

def generate_users(count=1000):
    """Generate synthetic MongoDB user data."""
    return [
        {
            "_id": str(uuid.uuid4().hex[:24]),
            "Uid": f"user_{i+1:06d}",
            "firstName": fake.first_name(),
            "lastName": fake.last_name(),
            "occupation": random.choice(OCCUPATIONS),
            "state": random.choice(NIGERIAN_STATES),
        }
        for i in range(count)
    ]


def generate_savings_plans(user_ids, count=500):
    """Generate synthetic savings plan records."""
    plans, start_date = [], datetime.now() - timedelta(days=730)

    for _ in range(count):
        plan_start = start_date + timedelta(days=random.randint(0, 700))
        duration = random.randint(30, 365)
        plan_end = plan_start + timedelta(days=duration)
        status = (
            random.choice(["completed", "cancelled"])
            if plan_end < datetime.now()
            else random.choice(["active", "active", "active", "paused"])
        )
        plans.append({
            "plan_id": str(uuid.uuid4()),
            "product_type": random.choice(PRODUCT_TYPES),
            "customer_uid": random.choice(user_ids),
            "amount": round(random.uniform(5_000, 1_000_000), 2),
            "frequency": random.choice(FREQUENCIES),
            "start_date": plan_start.date(),
            "end_date": plan_end.date(),
            "status": status,
            "created_at": plan_start,
            "updated_at": plan_start + timedelta(days=random.randint(0, 30)),
            "deleted_at": plan_end if status == "cancelled" else None,
        })
    return plans


def generate_savings_transactions(plan_ids, count=5000):
    """Generate synthetic savings transaction records."""
    transactions, start_date = [], datetime.now() - timedelta(days=730)

    for _ in range(count):
        txn_time = start_date + timedelta(
            days=random.randint(0, 730),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )
        currency = random.choice(CURRENCIES)
        rate = 1.0
        amount = 0.0

        if currency == "NGN":
            amount = round(random.uniform(1_000, 500_000), 2)
        else:
            amount = round(random.uniform(10, 5_000), 2)
            rate = round(random.uniform(
                {"USD": 1500, "GBP": 1900, "EUR": 1600}[currency],
                {"USD": 1650, "GBP": 2100, "EUR": 1800}[currency]
            ), 2)

        deleted_at = txn_time + timedelta(hours=random.randint(1, 48)) if random.random() < 0.05 else None

        transactions.append({
            "txn_id": str(uuid.uuid4()),
            "plan_id": random.choice(plan_ids),
            "amount": amount,
            "currency": currency,
            "side": random.choice(TRANSACTION_SIDES),
            "rate": rate,
            "txn_timestamp": txn_time,
            "updated_at": txn_time,
            "deleted_at": deleted_at,
        })
    return transactions

# DATABASE OPERATIONS

def insert_users_to_mongodb(users):
    """Insert generated users into MongoDB Atlas."""
    print("Connecting to MongoDB Atlas...")
    client = MongoClient(MONGO_URI)
    db = client["nomba_users"]
    coll = db["nomba"]

    coll.delete_many({})
    coll.insert_many(users)
    print(f"Inserted {len(users)} users into MongoDB collection 'nomba_users.nomba'")
    client.close()


def insert_data_to_postgres(plans, transactions):
    """Insert generated savings plans and transactions into PostgreSQL."""
    print("Connecting to PostgreSQL (Aiven)...")
    conn = psycopg2.connect(POSTGRES_SOURCE_URI)
    cur = conn.cursor()

    # Create tables if they do not exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS savings_plan (
            plan_id UUID PRIMARY KEY,
            product_type TEXT,
            customer_uid TEXT,
            amount NUMERIC(15, 2),
            frequency TEXT,
            start_date DATE,
            end_date DATE,
            status TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            deleted_at TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS savingstransaction (
            txn_id UUID PRIMARY KEY,
            plan_id UUID,
            amount NUMERIC(15, 2),
            currency TEXT,
            side TEXT,
            rate NUMERIC(10, 2),
            txn_timestamp TIMESTAMP,
            updated_at TIMESTAMP,
            deleted_at TIMESTAMP
        )
    """)

    # Reset tables for clean data load
    cur.execute("TRUNCATE savingstransaction, savings_plan RESTART IDENTITY CASCADE")

    print(f"Inserting {len(plans)} savings plans and {len(transactions)} transactions...")

    execute_values(cur, """
        INSERT INTO savings_plan VALUES %s
    """, [tuple(p.values()) for p in plans])

    execute_values(cur, """
        INSERT INTO savingstransaction VALUES %s
    """, [tuple(t.values()) for t in transactions])

    conn.commit()

    # Data summary
    cur.execute("SELECT COUNT(*) FROM savings_plan WHERE deleted_at IS NULL")
    active_plans = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM savingstransaction WHERE deleted_at IS NULL")
    active_txns = cur.fetchone()[0]

    print("Data Summary:")
    print(f"Active Plans: {active_plans}")
    print(f"Active Transactions: {active_txns}")
    print(f"Soft-Deleted Transactions: {len(transactions) - active_txns}")

    cur.close()
    conn.close()


def generate_incremental_updates(user_count=5, txn_count=20):
    """Simulate incremental changes to test CDC logic."""
    print("Simulating incremental updates...")

    client = MongoClient(MONGO_URI)
    coll = client["nomba_users"]["nomba"]
    users = list(coll.find().limit(user_count))

    for u in users:
        update = {}
        if random.random() < 0.5:
            update["occupation"] = random.choice(OCCUPATIONS)
        if random.random() < 0.3:
            update["state"] = random.choice(NIGERIAN_STATES)
        if update:
            coll.update_one({"_id": u["_id"]}, {"$set": update})

    print(f"Updated {len(users)} MongoDB users.")

    conn = psycopg2.connect(POSTGRES_SOURCE_URI)
    cur = conn.cursor()
    cur.execute("SELECT plan_id FROM savings_plan WHERE status='active' LIMIT 50")
    plan_ids = [r[0] for r in cur.fetchall()]

    new_txns = generate_savings_transactions(plan_ids, txn_count)
    execute_values(cur, """
        INSERT INTO savingstransaction VALUES %s
    """, [tuple(t.values()) for t in new_txns])

    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted {txn_count} new transactions into PostgreSQL.")



# MAIN
def main():
    parser = argparse.ArgumentParser(description="Generate sample data for Nomba Data Engineer Assessment")
    parser.add_argument("--users", type=int, default=1000)
    parser.add_argument("--plans", type=int, default=500)
    parser.add_argument("--transactions", type=int, default=5000)
    parser.add_argument("--incremental", action="store_true")

    args = parser.parse_args()

    if args.incremental:
        generate_incremental_updates()
    else:
        users = generate_users(args.users)
        user_ids = [u["Uid"] for u in users]
        plans = generate_savings_plans(user_ids, args.plans)
        plan_ids = [p["plan_id"] for p in plans]
        transactions = generate_savings_transactions(plan_ids, args.transactions)

        insert_users_to_mongodb(users)
        insert_data_to_postgres(plans, transactions)

        print("Data generation complete. To simulate CDC, run:")
        print("python generate_sample_data.py --incremental")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit("Process interrupted by user.")
