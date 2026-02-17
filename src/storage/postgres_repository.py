import psycopg2
import os
import json
import hashlib

class PostgresRepository:

    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost",
            port=5432,
            dbname="copiloto_inversiones",
            user="postgres",
            password="postgres"
        )
        self.cur = self.conn.cursor()

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
