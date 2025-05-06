import os
import socket
import psycopg2
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# ─── CONFIG ───────────────────────────────────────────────────────────────
DATABASE_URL = os.getenv('NEON_DATABASE_URL')
if not DATABASE_URL:
    raise RuntimeError("NEON_DATABASE_URL not set")

hostname = socket.gethostname()
HOST = socket.gethostbyname(hostname)
PORT = int(input("Port to listen on: "))
BUFF_SIZE = 5098

# ─── HELPERS ──────────────────────────────────────────────────────────────
def to_pst(ts: datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(ZoneInfo('America/Los_Angeles')).isoformat()

def moisture_to_rh(raw: float) -> float:
    return max(0.0, min(100.0, raw / 1023.0 * 100.0))

def liters_to_gallons(liters: float) -> float:
    return liters * 0.264172

def parse_timestamp(ts: str) -> datetime:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc)

# ─── SIMPLE BST FOR SEARCHING ─────────────────────────────────────────────
class BSTNode:
    def __init__(self, key, data):
        self.key = key
        self.data = [data]
        self.left = None
        self.right = None

    def insert(self, key, data):
        if key == self.key:
            self.data.append(data)
        elif key < self.key:
            if self.left:
                self.left.insert(key, data)
            else:
                self.left = BSTNode(key, data)
        else:
            if self.right:
                self.right.insert(key, data)
            else:
                self.right = BSTNode(key, data)

    def get_range(self, start, end):
        results = []
        if self.left and start < self.key:
            results.extend(self.left.get_range(start, end))
        if start <= self.key <= end:
            results.extend(self.data)
        if self.right and end > self.key:
            results.extend(self.right.get_range(start, end))
        return results

# ─── METADATA ─────────────────────────────────────────────────────────────
DEVICE_METADATA = {
    "Fridge 1": {"tz": "UTC"},
    "Fridge 2": {"tz": "UTC"},
    "Dishwasher": {"tz": "UTC"},
}

# ─── QUERY HANDLERS ───────────────────────────────────────────────────────
def handle_avg_moisture(pg_conn):
    print('Client asked: "What is the average moisture inside my kitchen fridge in the past three hours?"')
    board_name = "Fridge 1 Board"
    since = datetime.now(timezone.utc) - timedelta(hours=3)

    bst = None
    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT payload
            FROM "Lab7_virtual"
            WHERE payload->>'board_name' = %s
        """, (board_name,))
        for row in cur.fetchall():
            payload = row[0]
            ts = parse_timestamp(payload["timestamp"])
            val = float(payload.get("Moisture Meter (Fridge 1)", 0.0))
            if bst:
                bst.insert(ts, val)
            else:
                bst = BSTNode(ts, val)

    readings = bst.get_range(since, datetime.now(timezone.utc)) if bst else []
    avg_raw = sum(readings) / len(readings) if readings else 0.0
    rh = moisture_to_rh(avg_raw)
    return f"Fridge avg RH% (3h): {rh:.2f}% at {to_pst(datetime.now(timezone.utc))}"

def handle_avg_water(pg_conn):
    print('Client asked: "What is the average water consumption per cycle in my smart dishwasher?"')
    board_name = "Dishwasher Board"

    total = 0.0
    count = 0
    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT payload
            FROM "Lab7_virtual"
            WHERE payload->>'board_name' = %s
        """, (board_name,))
        for row in cur.fetchall():
            payload = row[0]
            liters = float(payload.get("Water Consumption Sensor", 0.0))
            total += liters
            count += 1

    avg_liters = total / count if count else 0.0
    gallons = liters_to_gallons(avg_liters)
    return f"Dishwasher avg water/cycle: {gallons:.2f} gal at {to_pst(datetime.now(timezone.utc))}"

def handle_max_electricity(pg_conn):
    print('Client asked: "Which device consumed more electricity among my three iot devices (two refrigerators and a dishwasher)"')
    boards = {
        "Fridge 1 Board": "Ammeter (Fridge 1)",
        "Fridge 2 Board": "Ammeter (Fridge 2)",
        "Dishwasher Board": "Ammeter (Dishwasher)",
    }


    totals = {}
    with pg_conn.cursor() as cur:
        for board, sensor in boards.items():
            cur.execute("""
                SELECT payload
                FROM "Lab7_virtual"
                WHERE payload->>'board_name' = %s
            """, (board,))
            for row in cur.fetchall():
                payload = row[0]
                val = float(payload.get(sensor, 0.0))
                totals[board] = totals.get(board, 0.0) + val

    if not totals:
        return "No electricity data available."

    max_device = max(totals, key=totals.get)
    max_val = totals[max_device]
    return f"{max_device} consumed the most electricity: {max_val:.2f} kWh"

# ─── MAIN SERVER LOOP ─────────────────────────────────────────────────────
def main():
    pg_conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    print("[DB] Connected.")

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.bind((HOST, PORT))
    srv.listen(1)
    print(f"[TCP] Listening on {HOST}:{PORT}")

    client, addr = srv.accept()
    print("Connection from", addr)
    try:
        while True:
            code = client.recv(BUFF_SIZE).decode().strip()
            if not code:
                break

            with psycopg2.connect(DATABASE_URL, sslmode='require') as pg_conn:
                if code == '1':
                    resp = handle_avg_moisture(pg_conn)
                elif code == '2':
                    resp = handle_avg_water(pg_conn)
                elif code == '3':
                    resp = handle_max_electricity(pg_conn)
                else:
                    resp = "ERROR: send 1, 2, or 3."
            client.send(resp.encode())
            
    finally:
        client.close()
        srv.close()
        pg_conn.close()

if __name__ == "__main__":
    main()
