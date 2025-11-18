import csv
from pathlib import Path
from typing import Dict, Iterable, List

import mysql.connector
from mysql.connector import MySQLConnection


DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "database": "mydb",
    "user": "myuser",
    "password": "mypassword",
}

ROOT_DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "database": "mydb",
    "user": "root",
    "password": "root",
}


class AzkiSimulator:
    def __init__(self):
        self.conn = None
        self.cursor = None

    def connect(self) -> None:
        """Establish a connection to MySQL."""
        if self.conn:
            return

        print(f"🔌 Connecting to database ...")
        self.conn = mysql.connector.connect(**DB_CONFIG)
        self.cursor = self.conn.cursor()
        print("✅ Connected!")

    def connect_root(self) -> None:
        """Establish a connection to MySQL."""
        if self.conn:
            return

        print(f"🔌 Connecting to database ...")
        self.conn = mysql.connector.connect(**ROOT_DB_CONFIG)
        self.cursor = self.conn.cursor()
        print("✅ Connected!")

    def grant_user(self) -> None:
        self.cursor.execute("GRANT SELECT, RELOAD, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'myuser'@'%'")
        self.cursor.execute("FLUSH PRIVILEGES")
        print("🔐 Privileges updated.")

    def close(self) -> None:
        """Close cursor & connection safely."""
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.conn:
            self.conn.close()
            self.conn = None

    def ensure_table(self, table_name: str) -> None:
        ddl = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                user_id INT PRIMARY KEY,
                signup_date DATE,
                city VARCHAR(255),
                device_type VARCHAR(255)
            );
        """
        self.cursor.execute(ddl)

    def insert_rows(self, table_name: str, columns: List[str], rows: List[Dict[str, str]]) -> None:
        placeholders = ", ".join(["%s"] * len(columns))
        col_list = ", ".join(f"`{col}`" for col in columns)
        dml = f"INSERT INTO `{table_name}` ({col_list}) VALUES ({placeholders})"
        values = [[row.get(col) for col in columns] for row in rows]
        self.cursor.executemany(dml, values)

    def load_csv_into_table(self, csv_path: Path, table_name: str) -> None:
        """
        Read rows from `csv_path` and insert them into `table_name`.
        Table is created automatically if needed.
        """
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        with csv_path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            columns = reader.fieldnames or []
            rows = list(reader)

        if not rows:
            print(f"⚠️ CSV file {csv_path} is empty; nothing to import.")
            return

        self.ensure_table(table_name)
        self.insert_rows(table_name, columns, rows)
        self.conn.commit()
        print(f"📥 Inserted {len(rows)} rows into `{table_name}`.")


def grant_to_user():
    simulator = AzkiSimulator()
    simulator.connect_root()
    simulator.grant_user()


if __name__ == "__main__":
    simulator = AzkiSimulator()
    simulator.connect()
    try:
        csv_file = Path(__file__).with_name("users.csv")
        simulator.load_csv_into_table(csv_file, table_name="users")
    finally:
        simulator.close()

    grant_to_user()
