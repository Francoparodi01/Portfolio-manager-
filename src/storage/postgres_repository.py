import psycopg2
import os
import json
import hashlib
from datetime import datetime, timedelta


class PostgresRepository:

    def __init__(self):

        database_url = os.getenv("DATABASE_URL")

        if database_url:
            if database_url.startswith("postgresql+psycopg2://"):
                database_url = database_url.replace(
                    "postgresql+psycopg2://",
                    "postgresql://"
                )

            self.conn = psycopg2.connect(database_url)
        else:
            self.conn = psycopg2.connect(
                host="localhost",
                port=5432,
                dbname="cocos_inversiones",
                user="postgres",
                password="postgres"
            )

        self.conn.autocommit = True
        self.cur = self.conn.cursor()

    def get_portfolio_history(self, days: int = 90):

        since = datetime.utcnow() - timedelta(days=days)

        self.cur.execute("""
            SELECT timestamp, total_value
            FROM portfolio_snapshots
            WHERE timestamp >= %s
            ORDER BY timestamp ASC
        """, (since,))

        rows = self.cur.fetchall()

        return [
            {
                "timestamp": row[0],
                "total_value": float(row[1])
            }
            for row in rows
        ]

    def save_raw_snapshot(self, raw_data: dict):

        raw_json = json.dumps(raw_data)
        checksum = hashlib.sha256(raw_json.encode()).hexdigest()

        self.cur.execute("""
            INSERT INTO raw_snapshots (raw_data, checksum)
            VALUES (%s, %s)
            ON CONFLICT (checksum) DO NOTHING
            RETURNING id
        """, (raw_json, checksum))

        result = self.cur.fetchone()
        self.conn.commit()

        return result[0] if result else None

    def save_portfolio_snapshot(self, timestamp, total_value, raw_id):

        self.cur.execute("""
            INSERT INTO portfolio_snapshots (timestamp, total_value, raw_snapshot_id)
            VALUES (%s, %s, %s)
            RETURNING id
        """, (timestamp, total_value, raw_id))

        snapshot_id = self.cur.fetchone()[0]
        self.conn.commit()

        return snapshot_id