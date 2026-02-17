import psycopg2
from src.storage.data_storage import DataStorage


# Conexión Timescale
pg_conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="copiloto_inversiones",
    user="postgres",
    password="postgres"
)

pg_cur = pg_conn.cursor()

# Conexión SQLite actual
sqlite_db = DataStorage()

history = sqlite_db.get_portfolio_history(days=365)

print(f"Snapshots encontrados en SQLite: {len(history)}")

for snapshot in history:

    # Insert snapshot
    pg_cur.execute("""
        INSERT INTO portfolio_snapshot (timestamp, total_value)
        VALUES (%s, %s)
        RETURNING id
    """, (
        snapshot["timestamp"],
        snapshot["total_value"]
    ))

    snapshot_id = pg_cur.fetchone()[0]

    # Insert posiciones
    for pos in snapshot.get("positions", []):

        pg_cur.execute("""
            INSERT INTO positions_snapshot (
                snapshot_id,
                ticker,
                quantity,
                price,
                valuation,
                pnl_percent
            )
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            snapshot_id,
            pos.get("Ticker"),
            pos.get("Cantidad"),
            pos.get("PrecioActual"),
            pos.get("Valuacion"),
            pos.get("GananciaPorcentaje")
        ))

pg_conn.commit()

pg_cur.close()
pg_conn.close()

print("Migración completada.")
