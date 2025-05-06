import os
import socket
import psycopg2
import json

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# ─── CONFIG ───────────────────────────────────────────────────────────────
DATABASE_URL = os.getenv('NEON_DATABASE_URL')
if not DATABASE_URL:
    raise RuntimeError("NEON_DATABASE_URL not set")

hostname = socket.gethostname()
HOST = socket.gethostbyname(hostname)
print(f'Host: {HOST}')
PORT      = int(input("Port to listen on: "))
BUFF_SIZE = 5098

# ─── HELPERS ──────────────────────────────────────────────────────────────
def to_pst(ts: datetime, src_tz: str = 'UTC') -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=ZoneInfo(src_tz))
    return ts.astimezone(ZoneInfo('America/Los_Angeles')).isoformat()

def moisture_to_rh(raw: float) -> float:
    return max(0.0, min(100.0, raw / 1023.0 * 100.0))

def liters_to_gallons(liters: float) -> float:
    return liters * 0.264172

# ─── LOAD METADATA ────────────────────────────────────────────────────────
def load_metadata(pg_conn):
    with pg_conn.cursor() as cur:
        # Introspect once (remove afterward)
        cur.execute("""
            SELECT table_name
              FROM information_schema.tables
             WHERE table_schema='public';
        """)
        print("Tables:", [r[0] for r in cur.fetchall()])

        cur.execute("""
            SELECT column_name
              FROM information_schema.columns
             WHERE table_name='appliances_metadata';
        """)
        print("appliances_metadata columns:", [r[0] for r in cur.fetchall()])

        # Load assetUid + customAttributes
        cur.execute("""
            SELECT "assetUid", "customAttributes"
              FROM appliances_metadata
        """)
        rows = cur.fetchall()

    meta = {}
    for asset_uid, custom in rows:
        try:
            attrs = json.loads(custom)
        except Exception:
            attrs = {}
        tz   = attrs.get('timezone', 'UTC')
        unit = attrs.get('unitOfMeasure') or attrs.get('unit') or ''
        meta[asset_uid] = {'tz': tz, 'unit': unit}
    return meta

# ─── QUERY HANDLERS ───────────────────────────────────────────────────────
def handle_avg_moisture(pg_conn, meta):
    device = 'kitchen_fridge'
    md     = meta[device]
    since  = datetime.utcnow() - timedelta(hours=3)

    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT "payload", "time"
              FROM appliances_virtual
             WHERE "payload"->>'assetUid' = %s
               AND "time" >= %s
        """, (device, since))
        recs = cur.fetchall()

    vals = [json.loads(p).get('moisture', 0.0) for p, _ in recs]
    avg_raw = sum(vals)/len(vals) if vals else 0.0
    ts      = recs[-1][1] if recs else datetime.utcnow()

    rh     = moisture_to_rh(avg_raw)
    pst_ts = to_pst(ts, md['tz'])
    return f"Fridge avg RH% (3h): {rh:.2f}% at {pst_ts}"

def handle_avg_water(pg_conn, meta):
    device = 'smart_dishwasher'
    md     = meta[device]

    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT "payload", "time"
              FROM appliances_virtual
             WHERE "payload"->>'assetUid' = %s
        """, (device,))
        recs = cur.fetchall()

    vals = [json.loads(p).get('water_liters', 0.0) for p, _ in recs]
    avg_l = sum(vals)/len(vals) if vals else 0.0
    ts    = recs[-1][1] if recs else datetime.utcnow()

    gal    = liters_to_gallons(avg_l)
    pst_ts = to_pst(ts, md['tz'])
    return f"Dishwasher avg water/cycle: {gal:.2f} gal at {pst_ts}"

def handle_max_electricity(pg_conn, meta):
    devices = ['fridge1', 'fridge2', 'smart_dishwasher']
    ph       = ','.join('%s' for _ in devices)

    with pg_conn.cursor() as cur:
        cur.execute(f"""
            SELECT "payload", "time"
              FROM appliances_virtual
             WHERE "payload"->>'assetUid' IN ({ph})
        """, devices)
        recs = cur.fetchall()

    sums = {}
    last_ts = {}
    for p, t in recs:
        doc = json.loads(p)
        dev = doc.get('assetUid')
        kwh = float(doc.get('electricity_kwh', 0.0))
        sums[dev] = sums.get(dev, 0.0) + kwh
        last_ts[dev] = t

    if not sums:
        return "No electricity data."

    winner = max(sums, key=sums.get)
    total  = sums[winner]
    pst_ts = to_pst(last_ts[winner], meta[winner]['tz'])
    return f"{winner} highest electricity: {total:.2f} kWh at {pst_ts}"

# ─── MAIN SERVER LOOP ─────────────────────────────────────────────────────
def main():
    pg_conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    meta    = load_metadata(pg_conn)
    print(f"[DB] Loaded metadata for {len(meta)} devices.")

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
            if   code == '1':
                resp = handle_avg_moisture(pg_conn, meta)
            elif code == '2':
                resp = handle_avg_water(pg_conn, meta)
            elif code == '3':
                resp = handle_max_electricity(pg_conn, meta)
            else:
                resp = "ERROR: send 1, 2, or 3."
            client.send(resp.encode())
    finally:
        client.close()
        srv.close()
        pg_conn.close()

if __name__ == "__main__":
    main()
