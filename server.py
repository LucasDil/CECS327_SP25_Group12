import socket
import sqlite3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

DB_PATH   = 'dataniz.db'
BUFF_SIZE = 4096

# valid questions
Q1 = "what is the average moisture inside my kitchen fridge in the past three hours?"
Q2 = "what is the average water consumption per cycle in my smart dishwasher?"
Q3 = "which device consumed more electricity among my three iot devices (two refrigerators and a dishwasher)?"
VALID = {Q1, Q2, Q3}

# simple converters
def moisture_to_rh(raw):            return max(0, min(100, raw/1023*100))
def liters_to_gal(liters):         return liters * 0.264172
def to_pst(dt, tz='UTC'):
    if dt.tzinfo is None: dt = dt.replace(tzinfo=ZoneInfo(tz))
    return dt.astimezone(ZoneInfo('America/Los_Angeles'))

def load_metadata(conn):
    cur = conn.cursor()
    cur.execute("SELECT device_id, timezone, unit_of_measure FROM device_metadata")
    return {did: {'tz':tz,'unit':unit} for did, tz, unit in cur.fetchall()}

def handle(q, db, meta):
    now = datetime.utcnow()
    pst_now = to_pst(now).isoformat()
    cur = db.cursor()

    if q==Q1:
        md = meta['kitchen_fridge']
        since = now - timedelta(hours=3)
        cur.execute("SELECT AVG(value),MAX(timestamp) FROM sensor_data WHERE device_id=? AND measurement=? AND timestamp>=?",
                    ('kitchen_fridge', md['unit'], since))
        avg, ts = cur.fetchone()
        return f"Fridge RH%: {moisture_to_rh(avg or 0):.2f}% at {to_pst(ts,md['tz']).isoformat()}"

    if q==Q2:
        md = meta['smart_dishwasher']
        cur.execute("SELECT AVG(value),MAX(timestamp) FROM sensor_data WHERE device_id=? AND measurement=?",
                    ('smart_dishwasher', md['unit']))
        avg, ts = cur.fetchone()
        return f"Dishwasher water: {liters_to_gal(avg or 0):.2f} gal at {to_pst(ts,md['tz']).isoformat()}"

    if q==Q3:
        ids = ['fridge1','fridge2','smart_dishwasher']
        cur.execute(f"SELECT device_id,SUM(value) FROM sensor_data WHERE measurement='electricity_kwh' AND device_id IN ({','.join('?'*3)}) GROUP BY device_id",
                    ids)
        rows = cur.fetchall()
        winner, tot = max(rows, key=lambda x:x[1] or 0)
        return f"{winner} used {tot:.2f} kWh as of {pst_now}"

    return "ERROR"

def main():
    db   = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    meta = load_metadata(db)

    sock = socket.socket()
    sock.bind(('0.0.0.0', 5000))
    sock.listen(1)
    conn, _ = sock.accept()

    try:
        while True:
            q = conn.recv(BUFF_SIZE).decode().strip().lower()
            if not q: break
            if q not in VALID:
                resp = "Sorry—valid queries are:\n • "+ "\n • ".join(VALID)
            else:
                resp = handle(q, db, meta)
            conn.send(resp.encode())
    finally:
        conn.close()
        sock.close()
        db.close()

if __name__=='__main__':
    main()
