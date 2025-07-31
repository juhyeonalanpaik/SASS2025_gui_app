import os
import sys
import json
import threading
import signal
import queue
import traceback
import cv2
from datetime import datetime, timezone
from collections import defaultdict

import psycopg2
from psycopg2 import sql, extras
from flask import Flask, request, jsonify

# ---------------- Configuration ----------------
DB_CONFIG = {
    "dbname":   "sensorData",
    "user":     "alanpaik",
    "password": "password",
    "host":     "localhost",
    "port":     "5432"
}

# Desired sensor sampling rate in Hz (controls how often data is recorded per subject)
SAMPLE_RATE_HZ = 10.0

RAW_DEVICE_SUBJECT_MAP = {
    "f327b3b2-6e63-4e33-be0d-67895b57a9ca": 1,  # Rohan's phone
    "fa74299f-9ee8-48e7-9381-c46de923a6a5": 2,  # Jamie's phone
    "947886d2-c812-4aae-ad83-7b2e883e4efd": 3,  # Edric's phone
}
DEVICE_SUBJECT_MAP = {k.lower(): v for k, v in RAW_DEVICE_SUBJECT_MAP.items()}

RTSP_URL   = "rtsp://admin:winlab!1234@192.168.1.6:554/h264Preview_01_main"
VIDEO_DIR  = "/Volumes/T7 Shield/final_recordings"
VIDEO_NAME = None  # will be set per-run in main()

FLASK_HOST = "0.0.0.0"
FLASK_PORT = 8000

DEBUG_PRINT = False
REQUIRE_COMPLETE_SENSORS = False

# ---------------- Globals ----------------
stop_event = threading.Event()
sensor_insert_queue = queue.Queue(maxsize=5000)

subject_configs = {}               # subject_id -> {activity, placement}
CURRENT_DEVICE_SUBJECT = {}        # device_id -> subject_id mapping

# Cache for merging missing sensor values (avoid NULLs) AND for throttling sample rate
last_values = defaultdict(lambda: {
    "accelerometer": None,
    "gyroscope":     None,
    "orientation":   None,
    "gravity":       None,
    "last_ts":       None
})

app = Flask(__name__)

# ---------------- Helpers ----------------
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def create_subject_table(subject_id):
    table_name = f"sensor_logs_subject_{subject_id}"
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMPTZ NOT NULL,
                subject_id INT NOT NULL,
                accelerometer FLOAT8[],
                gyroscope FLOAT8[],
                orientation FLOAT8[],
                gravity FLOAT8[],
                phone_placement TEXT,
                activity TEXT
            );
        """).format(sql.Identifier(table_name))
    )
    conn.commit()
    cur.close()
    conn.close()
    return table_name

def ensure_reolink_table():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS reolink_frames (
            id SERIAL PRIMARY KEY,
            filename TEXT,
            frame_number BIGINT,
            timestamp TIMESTAMPTZ
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

def extract_ts(reading):
    ts = reading.get("timestamp") or reading.get("time") or reading.get("ts")
    if ts is None:
        raise KeyError("No timestamp")
    if isinstance(ts, str):
        ts = float(ts)
    if ts > 1e15:
        sec = ts / 1e9
    elif ts > 1e12:
        sec = ts / 1e3
    else:
        sec = ts
    return datetime.fromtimestamp(sec, tz=timezone.utc)

def vec3_xyz_or_pry(v):
    if v is None:
        return None
    if isinstance(v, dict):
        ks = set(v.keys())
        if {"x","y","z"} <= ks:
            return [v["x"], v["y"], v["z"]]
        if {"pitch","roll","yaw"} <= ks:
            return [v["pitch"], v["roll"], v["yaw"]]
        if {"alpha","beta","gamma"} <= ks:
            return [v["alpha"], v["beta"], v["gamma"]]
    return v

# ---------------- Flask Endpoints ----------------
@app.route("/configure", methods=["POST"])
def configure_subjects():
    try:
        cfgs = request.get_json(force=True)
        for c in cfgs:
            sid = int(c["subject_id"])
            subject_configs[sid] = {
                "activity": c["activity"],
                "placement": c["placement"]
            }
            create_subject_table(sid)
        return jsonify({"status":"ok","subjects":subject_configs}), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error":str(e)}), 400

@app.route("/map_subject", methods=["POST"])
def map_subject():
    try:
        b = request.get_json(force=True)
        dev = (b.get("deviceId") or "").lower()
        sid = int(b["subject_id"])
        if not dev:
            return jsonify({"error":"deviceId required"}), 400
        CURRENT_DEVICE_SUBJECT[dev] = sid
        act = b.get("activity"); plc = b.get("placement")
        if act or plc:
            subject_configs.setdefault(sid, {})
            if act: subject_configs[sid]["activity"] = act
            if plc: subject_configs[sid]["placement"] = plc
        create_subject_table(sid)
        return jsonify({"status":"ok","device":dev,"subject":sid}), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error":str(e)}), 500

@app.route("/data", methods=["POST"])
def receive_data():
    try:
        data = request.get_json(force=True)
        device_id = (data.get("deviceId") or "").lower()
        payload   = data.get("payload", [])

        subject_id = CURRENT_DEVICE_SUBJECT.get(device_id)
        if subject_id is None:
            subject_id = DEVICE_SUBJECT_MAP.get(device_id)
        if subject_id is None:
            return jsonify({"error": f"Device {device_id} not mapped. POST /map_subject first."}), 400

        cfg = subject_configs.get(subject_id)
        if not cfg:
            return jsonify({"error": f"No config for subject {subject_id}. POST /configure or /map_subject."}), 400

        activity  = cfg["activity"]
        placement = cfg["placement"]

        table_name = create_subject_table(subject_id)

        if DEBUG_PRINT and payload:
            print("DEBUG first reading:", payload[0])

        # Style B: batched by sensor name
        if payload and isinstance(payload[0], dict) and "name" in payload[0]:
            by_time = {}
            for r in payload:
                ts = extract_ts(r)
                entry = by_time.setdefault(ts, {
                    "accelerometer": None,
                    "gyroscope":     None,
                    "orientation":   None,
                    "gravity":       None
                })
                name = (r.get("name") or "").lower()
                vals = r.get("values") or r.get("value") or r
                if name in entry:
                    entry[name] = vec3_xyz_or_pry(vals)

            for ts, sensors in by_time.items():
                # skip if all sensor readings are missing
                if not any([sensors["accelerometer"], sensors["gyroscope"],
                            sensors["orientation"],   sensors["gravity"]]):
                    continue
                # throttle by sample rate
                last_ts = last_values[subject_id]["last_ts"]
                if last_ts is not None and (ts - last_ts).total_seconds() < (1.0 / SAMPLE_RATE_HZ):
                    continue
                last_values[subject_id]["last_ts"] = ts

                row = (
                    table_name,
                    ts,
                    subject_id,
                    sensors["accelerometer"],
                    sensors["gyroscope"],
                    sensors["orientation"],
                    sensors["gravity"],
                    placement,
                    activity,
                )
                try:
                    sensor_insert_queue.put_nowait(row)
                except queue.Full:
                    print("⚠️ Queue full, dropping sensor sample")

        # Style A: each reading is a full dict
        else:
            for reading in payload:
                ts = extract_ts(reading)
                acc  = vec3_xyz_or_pry(reading.get("accelerometer"))
                gyro = vec3_xyz_or_pry(reading.get("gyroscope"))
                ori  = vec3_xyz_or_pry(reading.get("orientation"))
                grav = vec3_xyz_or_pry(reading.get("gravity"))

                # skip if no sensor data present
                if not any([acc, gyro, ori, grav]):
                    continue
                # throttle by sample rate
                last_ts = last_values[subject_id]["last_ts"]
                if last_ts is not None and (ts - last_ts).total_seconds() < (1.0 / SAMPLE_RATE_HZ):
                    continue
                last_values[subject_id]["last_ts"] = ts

                row = (
                    table_name,
                    ts,
                    subject_id,
                    acc,
                    gyro,
                    ori,
                    grav,
                    placement,
                    activity,
                )
                try:
                    sensor_insert_queue.put_nowait(row)
                except queue.Full:
                    print("⚠️ Queue full, dropping sensor sample")

        return jsonify({"status": "queued", "records": len(payload)}), 200

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# ---------------- Threads ----------------
def db_writer_thread():
    conn = get_db_connection()
    cur = conn.cursor()
    BATCH = 500
    while not stop_event.is_set():
        batch = []
        try:
            item = sensor_insert_queue.get(timeout=1)
            batch.append(item)
        except queue.Empty:
            pass

        while len(batch) < BATCH:
            try:
                batch.append(sensor_insert_queue.get_nowait())
            except queue.Empty:
                break

        if not batch:
            continue

        try:
            by_table = {}
            for row in batch:
                table = row[0]
                by_table.setdefault(table, []).append(row)

            for table_name, rows in by_table.items():
                insert_sql = sql.SQL("""
                    INSERT INTO {} (
                        timestamp, subject_id,
                        accelerometer, gyroscope,
                        orientation, gravity,
                        phone_placement, activity
                    ) VALUES %s
                """).format(sql.Identifier(table_name))

                values = [r[1:] for r in rows]
                extras.execute_values(cur, insert_sql, values,
                                     template="(%s,%s,%s,%s,%s,%s,%s,%s)")

            conn.commit()
            if DEBUG_PRINT:
                print(f"DB writer: inserted {len(batch)} rows.")
        except Exception as e:
            print("DB writer error:", e)
            traceback.print_exc()
            conn.rollback()

    cur.close()
    conn.close()
    print("DB writer thread stopped.")

def reolink_capture_thread():
    try:
        os.makedirs(VIDEO_DIR, exist_ok=True)
        ensure_reolink_table()
        conn = get_db_connection()
        cur = conn.cursor()

        path = os.path.join(VIDEO_DIR, VIDEO_NAME)
        cap  = cv2.VideoCapture(RTSP_URL, cv2.CAP_FFMPEG)
        cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        if not cap.isOpened():
            stop_event.set()
            return

        fps = cap.get(cv2.CAP_PROP_FPS) or 20.0
        HD_W, HD_H = 1280, 720
        out = cv2.VideoWriter(
            path,
            cv2.VideoWriter_fourcc(*"XVID"),
            fps,
            (HD_W, HD_H)
        )
        print("📹 REOLINK: Recording started.")

        frame_count = 0
        while not stop_event.is_set():
            ret, frame = cap.read()
            if not ret:
                break

            timestamp = datetime.utcnow().replace(tzinfo=timezone.utc)
            frame = cv2.rotate(frame, cv2.ROTATE_180)
            frame = cv2.resize(frame, (HD_W, HD_H),
                               interpolation=cv2.INTER_AREA)
            out.write(frame)
            frame_count += 1

            try:
                cur.execute(
                    "INSERT INTO reolink_frames (filename, frame_number, timestamp) VALUES (%s,%s,%s)",
                    (VIDEO_NAME, frame_count, timestamp)
                )
                conn.commit()
            except Exception as e:
                print("❌ DB Insert Error (Reolink frames):", e)
                traceback.print_exc()
                conn.rollback()

        cap.release()
        out.release()
        cur.close()
        conn.close()
    except Exception:
        traceback.print_exc()
        stop_event.set()

def main():
    if len(sys.argv) != 2:
        print('Usage: python multiple_sensorloggers_postgresql2.py "[{\\"subject_id\\":1,\\"activity\\":\\"normal_walk\\",\\"placement\\":\\"left_pocket\\"}, ...]"')
        sys.exit(1)

    global VIDEO_NAME
    VIDEO_NAME = "reolink_capture_" + datetime.utcnow().strftime("%Y%m%d_%H%M%S") + ".avi"

    # Map GUI slots → device UUIDs → subject_ids
    device_ids = [k for k, _ in sorted(DEVICE_SUBJECT_MAP.items(), key=lambda kv: kv[1])]
    subjects   = json.loads(sys.argv[1])
    for idx, cfg in enumerate(subjects):
        if idx >= len(device_ids):
            break
        sid = int(cfg["subject_id"])
        subject_configs[sid] = {
            "activity":  cfg["activity"],
            "placement": cfg["placement"]
        }
        CURRENT_DEVICE_SUBJECT[device_ids[idx]] = sid
        create_subject_table(sid)

    # Start background threads
    threading.Thread(target=db_writer_thread,         daemon=True).start()
    threading.Thread(target=reolink_capture_thread,   daemon=True).start()

    def run_flask():
        app.run(host=FLASK_HOST, port=FLASK_PORT,
                threaded=True, use_reloader=False)
    threading.Thread(target=run_flask, daemon=True).start()

    print("System running. Press Ctrl+C to stop.")
    signal.signal(signal.SIGINT,  lambda s,f: stop_event.set())
    signal.signal(signal.SIGTERM, lambda s,f: stop_event.set())
    stop_event.wait()

if __name__ == "__main__":
    main()

