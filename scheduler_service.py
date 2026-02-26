import schedule
import time
import os
from datetime import datetime
import pytz
from dotenv import load_dotenv

from run_collector import main as run_collector

load_dotenv()

TZ = pytz.timezone(os.getenv("TIMEZONE", "America/Argentina/Buenos_Aires"))
SNAPSHOT_TIMES = os.getenv("SNAPSHOT_TIMES", "17:30")

def job():
    now = datetime.now(TZ)
    print(f"Running snapshot at {now}")
    run_collector()

# Registrar múltiples horarios
for t in SNAPSHOT_TIMES.split(","):
    t = t.strip()
    schedule.every().day.at(t).do(job)
    print(f"Snapshot programado a las {t}")

print("Scheduler iniciado...")

while True:
    schedule.run_pending()
    time.sleep(30)
