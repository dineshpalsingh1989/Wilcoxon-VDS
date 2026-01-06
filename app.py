import json
import numpy as np
import paho.mqtt.client as mqtt
from collections import deque
from dash import Dash, dcc, html, dash_table
from dash.dependencies import Output, Input, State, ALL
import plotly.graph_objs as go
from threading import Thread, Lock
import dash_bootstrap_components as dbc
import time
from datetime import datetime, timedelta
from dash import no_update, ctx
import copy
import os
import psutil
import socket
from flask import send_file
import sqlite3
import pickle
import asyncio
import websockets
from queue import Queue, Empty
import subprocess
import re
from scipy.signal import find_peaks
from dash_extensions.WebSocket import WebSocket
import pandas as pd
import io
import uuid
import openpyxl
import ssl
import sys
import pytz

# ==========================================================
# --- CRITICAL DEPENDENCY CHECK ---
# ==========================================================
# Ensure you have installed: pip install pymodbus==2.5.3
# Pymodbus v3.x has a different API and will crash this script.

# ==========================================================
# --- SQLITE ADAPTERS ---
# ==========================================================
def adapt_datetime_iso(val):
    return val.isoformat()
def convert_datetime_iso(val):
    return datetime.fromisoformat(val.decode('utf-8'))

sqlite3.register_adapter(datetime, adapt_datetime_iso)
sqlite3.register_converter("datetime", convert_datetime_iso)
sqlite3.register_converter("timestamp", convert_datetime_iso)

# =========================
# --- MODBUS IMPORTS ---
# =========================
import struct
from pymodbus.server.sync import StartTcpServer
from pymodbus.device import ModbusDeviceIdentification
from pymodbus.datastore import ModbusSequentialDataBlock
from pymodbus.datastore import ModbusSlaveContext, ModbusServerContext

# =========================
# --- OPC UA IMPORTS ---
# =========================
from asyncua import Server, ua

# =========================
# MQTT CONFIG
# =========================
CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))
MQTT_CONFIG_FILE = os.path.join(CONFIG_DIR, "mqtt_config.json")
DEFAULT_CONFIG = {
    "PROTOCOL": "mqtt://", 
    "BROKER": "192.168.0.104",
    "PORT": 1883,
    "TOPIC": "wilcoxon",
    "USERNAME": "",
    "PASSWORD": ""
}
def load_mqtt_config():
    if not os.path.exists(MQTT_CONFIG_FILE):
        try:
            with open(MQTT_CONFIG_FILE, 'w') as f:
                json.dump(DEFAULT_CONFIG, f, indent=4)
            return DEFAULT_CONFIG
        except Exception as e:
            return DEFAULT_CONFIG
    try:
        with open(MQTT_CONFIG_FILE, 'r') as f:
            config = json.load(f)
            for key, value in DEFAULT_CONFIG.items():
                if key not in config:
                    config[key] = value
            return config
    except Exception as e:
        return DEFAULT_CONFIG

mqtt_config = load_mqtt_config()
BROKER = mqtt_config.get("BROKER", DEFAULT_CONFIG["BROKER"])
PORT = mqtt_config.get("PORT", DEFAULT_CONFIG["PORT"])
TOPIC = mqtt_config.get("TOPIC", DEFAULT_CONFIG["TOPIC"])
USERNAME = mqtt_config.get("USERNAME", DEFAULT_CONFIG["USERNAME"])
PASSWORD = mqtt_config.get("PASSWORD", DEFAULT_CONFIG["PASSWORD"])
PROTOCOL = mqtt_config.get("PROTOCOL", DEFAULT_CONFIG["PROTOCOL"]) 

# =========================
# MODBUS TCP CONFIG
# =========================
MODBUS_CONFIG_FILE = os.path.join(CONFIG_DIR, "modbus_config.json")
DEFAULT_MODBUS_CONFIG = {
    "enabled": False,
    "registers": []
}
def load_modbus_config():
    if not os.path.exists(MODBUS_CONFIG_FILE):
        try:
            with open(MODBUS_CONFIG_FILE, 'w') as f:
                json.dump(DEFAULT_MODBUS_CONFIG, f, indent=4)
            return DEFAULT_MODBUS_CONFIG
        except Exception:
            return DEFAULT_MODBUS_CONFIG
    try:
        with open(MODBUS_CONFIG_FILE, 'r') as f:
            config = json.load(f)
            return config
    except Exception:
        return DEFAULT_MODBUS_CONFIG
def save_modbus_config(config_data):
    try:
        with open(MODBUS_CONFIG_FILE, 'w') as f:
            json.dump(config_data, f, indent=4)
    except Exception:
        pass

# =========================
# OPC UA CONFIG
# =========================
OPCUA_PORT = 4840
OPCUA_START_TIME = datetime.now() # For Uptime tracking
opcua_server = None
opcua_node_map = {} 
opcua_running = False
opcua_enabled_flag = True 

# =========================
# GLOBAL DATA STORAGE & LOCK
# =========================
MAX_TREND_POINTS = 15
PEAK_HEIGHT_THRESHOLD = 0.1
script_dir = os.path.dirname(os.path.abspath(__file__))
DATA_FOLDER = os.path.join(script_dir, "device_data")
DB_FILE = os.path.join(DATA_FOLDER, "vibration_trends.db")
data_lock = Lock()
data_storage = {}
initial_device_config = {}
mqtt_status_lock = Lock()
mqtt_connection_status = {"status": "disconnected", "message": "Initializing..."}
modbus_server_context = None 
job_lock = Lock()
active_jobs = {} 
websocket_clients = set()
update_queue = Queue()
WEBSOCKET_PORT = 8051
log_lock = Lock()
log_messages = deque(maxlen=100) 
TRENDED_METRICS = [
    "Acceleration", "Velocity", "Displacement",
    "Crest Factor", "True Peak", "Std Deviation"
]

# =========================
# DATABASE HELPER FUNCTIONS
# =========================
def log_to_web(message):
    print(message)
    try:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with log_lock:
            log_messages.appendleft(f"[{timestamp}] {message}")
    except Exception:
        pass

def get_db_connection():
    conn = sqlite3.connect(
        DB_FILE,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        timeout=10 
    )
    try:
        # FIX 4: Optimize SQLite for High Concurrency
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
    except Exception as e:
        pass
    return conn

def setup_database():
    os.makedirs(DATA_FOLDER, exist_ok=True)
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS trends (
            timestamp DATETIME, gateway_id TEXT NOT NULL, channel TEXT NOT NULL,
            metric_name TEXT NOT NULL, value REAL, rpm REAL,
            PRIMARY KEY (timestamp, gateway_id, channel, metric_name)
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_trends_query
        ON trends (gateway_id, channel, metric_name, timestamp)
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_waveforms (
            gateway_id TEXT NOT NULL, channel TEXT NOT NULL, timestamp DATETIME,
            waveform_data BLOB, sampling_freq INTEGER,
            PRIMARY KEY (gateway_id, channel, timestamp)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_spectrum (
            gateway_id TEXT NOT NULL, channel TEXT NOT NULL, timestamp DATETIME,
            spectrum_data BLOB, bandwidth REAL, lines INTEGER,
            PRIMARY KEY (gateway_id, channel, timestamp)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS config (
            gateway_id TEXT PRIMARY KEY, config_data BLOB
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS trends_latest (
            gateway_id TEXT NOT NULL, channel TEXT NOT NULL,
            metric_name TEXT NOT NULL, value REAL, rpm REAL,
            timestamp DATETIME,
            PRIMARY KEY (gateway_id, channel, metric_name)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS recording_jobs (
            job_id TEXT PRIMARY KEY,
            gateway_id TEXT NOT NULL,
            start_time DATETIME NOT NULL,
            end_time DATETIME NOT NULL,
            duration_hours INTEGER NOT NULL,
            status TEXT NOT NULL 
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_jobs_gateway_status
        ON recording_jobs (gateway_id, status)
    """)
    conn.commit()
    conn.close()
    log_to_web(f"✅ SQLite Database setup complete at {DB_FILE}")

def save_device_config_db(gateway_id, config_dict):
    conn = get_db_connection()
    try:
        config_blob = pickle.dumps(config_dict)
        conn.execute(
            "INSERT OR REPLACE INTO config (gateway_id, config_data) VALUES (?, ?)",
            (gateway_id, config_blob)
        )
        conn.commit()
    except Exception as e: log_to_web(f"❌ Error saving device config: {e}")
    finally: conn.close()

def load_device_config_db():
    conn = get_db_connection()
    configs = {}
    try:
        cursor = conn.execute("SELECT gateway_id, config_data FROM config")
        for row in cursor.fetchall():
            gateway_id, config_blob = row
            if config_blob:
                configs[gateway_id] = pickle.loads(config_blob)
            else:
                configs[gateway_id] = {}
    except Exception: pass
    finally: conn.close()
    return configs

def load_latest_metrics_from_db(gateway_id, channel, conn):
    cursor = conn.execute(
        """
        SELECT metric_name, value, rpm, strftime('%Y-%m-%d %H:%M:%S', timestamp), timestamp
        FROM trends_latest 
        WHERE gateway_id=? AND channel=?
        """,
        (gateway_id, channel)
    )
    rows = cursor.fetchall()
    if not rows:
        return None, None, None, None
    latest_metrics = {}
    latest_ts_str = "N/A"
    latest_rpm = 0
    latest_dt = None
    for row in rows:
        metric_ts = row[4]
        if latest_dt is None or (metric_ts and metric_ts > latest_dt):
            latest_dt = metric_ts
            latest_ts_str = row[3]
            latest_rpm = row[2]
    for row in rows:
        latest_metrics[row[0]] = row[1]
    sensor_sensitivity = 100 
    return latest_metrics, latest_ts_str, latest_rpm, sensor_sensitivity

def load_trends_from_db(gateway_id, channel, conn):
    trends = {m: deque(maxlen=MAX_TREND_POINTS) for m in TRENDED_METRICS}
    trend_times = deque(maxlen=MAX_TREND_POINTS)
    try:
        time_cursor = conn.execute("""
            SELECT DISTINCT timestamp FROM trends
            WHERE gateway_id = ? AND channel = ?
            ORDER BY timestamp DESC LIMIT ?
        """, (gateway_id, channel, MAX_TREND_POINTS))
        target_timestamps = sorted([row[0] for row in time_cursor.fetchall()])

        if target_timestamps:
            placeholders = ','.join('?' for _ in target_timestamps)
            query = f"""
                SELECT strftime('%Y-%m-%d %H:%M:%S', timestamp), metric_name, value
                FROM trends WHERE gateway_id=? AND channel=? AND timestamp IN ({placeholders})
                ORDER BY timestamp ASC
            """
            params = [gateway_id, channel] + target_timestamps
            data_cursor = conn.execute(query, params)
            processed_times = {}
            for ts_str, metric_name, value in data_cursor.fetchall():
                if metric_name in trends: trends[metric_name].append(value)
                if ts_str not in processed_times: processed_times[ts_str] = True
            trend_times.extend(processed_times.keys())
    except Exception: pass
    return trends, trend_times

def load_data_from_db_on_startup():
    global data_storage, initial_device_config, active_jobs
    setup_database()
    initial_device_config = load_device_config_db()
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM trends_latest")
        latest_count = cursor.fetchone()[0]
        if latest_count == 0:
            cursor.execute("SELECT COUNT(*) FROM trends")
            old_count = cursor.fetchone()[0]
            if old_count > 0:
                log_to_web(f"ℹ️ Migrating {old_count} legacy records...")
                migration_query = """
                INSERT OR IGNORE INTO trends_latest 
                    (gateway_id, channel, metric_name, value, rpm, timestamp)
                SELECT 
                    gateway_id, channel, metric_name, value, rpm, timestamp
                FROM (
                    SELECT *, ROW_NUMBER() OVER(
                            PARTITION BY gateway_id, channel, metric_name 
                            ORDER BY timestamp DESC
                        ) as rn
                    FROM trends
                )
                WHERE rn = 1;
                """
                cursor.execute(migration_query)
                conn.commit()
    except Exception as e:
        conn.rollback()

    try:
        with job_lock:
            active_jobs.clear()
            cursor = conn.execute("SELECT job_id, gateway_id, end_time FROM recording_jobs WHERE status='active'")
            now = datetime.now()
            for job_id, gateway_id, end_time in cursor.fetchall():
                if end_time > now:
                    active_jobs[gateway_id] = {'job_id': job_id, 'end_time': end_time}
                else:
                    conn.execute("UPDATE recording_jobs SET status='completed' WHERE job_id=?", (job_id,))
            conn.commit()
            log_to_web(f"✅ Loaded {len(active_jobs)} active recording jobs.")
        
        with data_lock:
            all_gids = initial_device_config.keys()
            for gid in all_gids:
                data_storage[gid] = {
                    "channels": {}, "rpm": 0.0, "last_seen": "N/A",
                    "rpm_trend": deque(maxlen=MAX_TREND_POINTS),
                    "rpm_trend_times": deque(maxlen=MAX_TREND_POINTS)
                    }
                latest_device_ts = "N/A"
                latest_device_rpm = 0.0
                for ch in ['1', '2', '3', '4']:
                    metrics, ts_str, rpm, sens = load_latest_metrics_from_db(gid, ch, conn)
                    trends, trend_times = load_trends_from_db(gid, ch, conn)
                    data_storage[gid]["channels"][ch] = {
                        "metrics": metrics if metrics else {},
                        "timestamp": ts_str if ts_str else "N/A",
                        "sensitivity": sens if sens else 100,
                        "trends": trends, "trend_times": trend_times,
                        "waveform_data": [], "spectrum_data": [],
                        "sampling_freq": 12800, "bandwidth": 5000, "lines": 6400
                    }
                    if ts_str:
                        if latest_device_ts == "N/A" or ts_str > latest_device_ts:
                            latest_device_ts = ts_str
                            latest_device_rpm = rpm
                data_storage[gid]["last_seen"] = latest_device_ts
                data_storage[gid]["rpm"] = latest_device_rpm
    except Exception: pass
    finally: conn.close()
    return initial_device_config

def format_time_remaining(end_time):
    now = datetime.now()
    if end_time < now:
        return "Completed"
    delta = end_time - now
    total_seconds = int(delta.total_seconds())
    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    if days > 0: return f"{days}d, {hours}h, {minutes}m remaining"
    elif hours > 0: return f"{hours}h, {minutes}m remaining"
    elif minutes > 0: return f"{minutes}m, {seconds}s remaining"
    else: return f"{seconds}s remaining"

def generate_data_storage_layout():
    all_gateway_ids = []
    conn = get_db_connection()
    try:
        cursor = conn.execute("SELECT gateway_id FROM config ORDER BY gateway_id")
        all_gateway_ids = [row[0] for row in cursor.fetchall()]
    except Exception: pass
    finally: conn.close()

    gateway_options = [{'label': gid, 'value': gid} for gid in all_gateway_ids]

    control_card = dbc.Card([
        dbc.CardHeader("Data Recording Management"),
        dbc.CardBody([
            dbc.Row([
                dbc.Col(
                    [
                        dbc.Label("Select Serial Number (Gateway):", className="fw-bold"),
                        dcc.Dropdown(
                            id='data-storage-gateway-select',
                            options=gateway_options,
                            placeholder="Select a serial number...",
                            value=gateway_options[0]['value'] if gateway_options else None
                        ),
                    ], md=12, className="mb-3"
                ),
            ]),
            html.Hr(),
            dcc.Loading(
                id="loading-data-storage-panel",
                type="default",
                children=html.Div(id='data-storage-controls-container')
            )
        ])
    ], className="mb-4")
    
    db_file_card = dbc.Card(className="mb-4")
    try:
        stat = os.stat(DB_FILE)
        filename = os.path.basename(DB_FILE)
        file_body = dbc.CardBody([
            dbc.ListGroup([
                dbc.ListGroupItem(f"File Name: {filename}"),
                dbc.ListGroupItem(f"Total File Size: {format_size(stat.st_size)}"),
                dbc.ListGroupItem(f"Last Modified: {datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')}")
            ], flush=True, className="mb-3"),
            dbc.Button([html.I(className="fas fa-download me-2"), "Download Database File"], 
                        href=f"/download/{filename}", 
                        color="primary", 
                        outline=True, 
                        external_link=True)
        ])
    except FileNotFoundError:
        file_body = dbc.CardBody(dbc.Alert("Database file (vibration_trends.db) not found.", color="warning"))
    
    db_file_card = dbc.Card([
        dbc.CardHeader([html.I(className="fas fa-file-alt me-2"), "Database File Management"]),
        file_body
    ], className="mb-4")

    return html.Div([
        dbc.Row(dbc.Col(html.H4("Data Recording"), className="mb-4 mt-4")),
        dbc.Row(dbc.Col(dbc.Alert([
            html.I(className="fas fa-info-circle me-2"),
            "Select a device serial number to start a new recording or manage completed recordings."
        ], color="info"), width=12)),
        dbc.Row(dbc.Col(control_card, lg=12)),
        html.Hr(className="my-4"),
        dbc.Row(dbc.Col(html.H4("Database File"), className="mb-4")),
        dbc.Row(dbc.Col(db_file_card, lg=6))
    ])

def format_size(size_bytes):
    if size_bytes >= 1024**3: return f"{size_bytes / 1024**3:.2f} GB"
    elif size_bytes >= 1024**2: return f"{size_bytes / 1024**2:.2f} MB"
    elif size_bytes >= 1024: return f"{size_bytes / 1024:.2f} KB"
    else: return f"{size_bytes} Bytes"

def get_pi_serial():
    try:
        with open('/proc/cpuinfo', 'r') as f:
            for line in f:
                if line.startswith('Serial'): return line.split(':')[1].strip()
    except Exception: return "N/A"
    return "N/A"
def get_uptime():
    try:
        boot_time_timestamp = psutil.boot_time()
        elapsed_time = datetime.now() - datetime.fromtimestamp(boot_time_timestamp)
        total_seconds = int(elapsed_time.total_seconds())
        days, remainder = divmod(total_seconds, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, seconds = divmod(remainder, 60)
        if days > 0: return f"{days} days, {hours:02}:{minutes:02}:{seconds:02}"
        else: return f"{hours:02}:{minutes:02}:{seconds:02}"
    except Exception: return "N/A"
def get_memory_usage():
    try:
        mem = psutil.virtual_memory()
        total_mb = mem.total / (1024 * 1024); free_mb = mem.available / (1024 * 1024)
        return f"{total_mb:.0f} MB", f"{free_mb:.0f} MB"
    except Exception: return "N/A", "N/A"
def get_cpu_temp():
    try:
        if hasattr(psutil, 'sensors_temperatures'):
            temps = psutil.sensors_temperatures()
            if 'cpu_thermal' in temps: return f"{temps['cpu_thermal'][0].current:.1f}°C"
            for name in ['cpu-thermal', 'thermal_zone0']:
                if name in temps: return f"{temps[name][0].current:.1f}°C"
        try:
            with open('/sys/class/thermal/thermal_zone0/temp', 'r') as f:
                temp = int(f.read().strip()) / 1000.0; return f"{temp:.1f}°C"
        except FileNotFoundError: pass
    except Exception: pass
    return "N/A"
def get_disk_usage():
    try:
        disk = psutil.disk_usage('/')
        total_gb = disk.total / (1024 * 1024 * 1024); free_gb = disk.free / (1024 * 1024 * 1024)
        return f"{total_gb:.1f} GB", f"{free_gb:.1f} GB"
    except Exception: return "N/A", "N/A"
def get_cpu_load():
    try: return f"{psutil.cpu_percent(interval=None):.1f}%"
    except Exception: return "N/A"

def get_ip_address():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(0.2) 
    try:
        s.connect(('8.8.8.8', 1))
        IP = s.getsockname()[0]
    except Exception:
        try:
            IP = socket.gethostbyname(socket.gethostname())
        except Exception:
            IP = '127.0.0.1' 
    finally:
        s.close()
    return IP

def get_ethernet_settings():
    settings = {
        "ip_address": "N/A",
        "subnet_mask": "N/A",
        "gateway": "N/A",
        "dns_servers": "N/A"
    }
    try:
        ip_output = subprocess.run(['ip', 'addr', 'show', 'eth0'], capture_output=True, text=True, check=True).stdout
        ip_match = re.search(r'inet ([\d\.]+)/(\d+)', ip_output)
        if ip_match:
            settings["ip_address"] = ip_match.group(1)
            prefix = int(ip_match.group(2))
            mask = (0xFFFFFFFF << (32 - prefix)) & 0xFFFFFFFF
            settings["subnet_mask"] = f"{(mask >> 24) & 0xFF}.{(mask >> 16) & 0xFF}.{(mask >> 8) & 0xFF}.{mask & 0xFF}"

        route_output = subprocess.run(['ip', 'route', 'show', 'default'], capture_output=True, text=True, check=True).stdout
        gw_match = re.search(r'default via ([\d\.]+)', route_output)
        if gw_match:
            settings["gateway"] = gw_match.group(1)
        
        resolv_output = subprocess.run(['cat', '/etc/resolv.conf'], capture_output=True, text=True, check=True).stdout
        dns_matches = re.findall(r'nameserver ([\d\.]+)', resolv_output)
        if dns_matches:
            settings["dns_servers"] = ", ".join(dns_matches)
            
    except Exception as e:
        log_to_web(f"⚠️ Could not read network settings: {e}")
    
    if settings["ip_address"] == "N/A":
        settings["ip_address"] = get_ip_address()

    return settings

def get_system_time_settings():
    options = [{'label': 'UTC', 'value': 'UTC'}]
    current_tz = 'UTC'
    ntp_on = False
    
    try:
        result_list = subprocess.run(['timedatectl', 'list-timezones'], capture_output=True, text=True, check=True)
        timezones = result_list.stdout.strip().split('\n')
        options = [{'label': tz, 'value': tz} for tz in timezones if tz.strip()]
        
        result_status = subprocess.run(['timedatectl', 'show'], capture_output=True, text=True, check=True)
        status_lines = result_status.stdout.strip().split('\n')
        
        for line in status_lines:
            if line.startswith('Timezone='):
                current_tz = line.split('=')[1]
            if line.startswith('NTP='):
                ntp_on = (line.split('=')[1] == 'yes')
                
        if 'NTPSynchronized=yes' in result_status.stdout:
            ntp_on = True

    except Exception:
        if 'Asia/Kuala_Lumpur' not in [opt['value'] for opt in options]:
            options.append({'label': '(UTC+08:00) Asia/Kuala_Lumpur', 'value': 'Asia/Kuala_Lumpur'})
    
    return options, current_tz, ntp_on

SERVER_IP = get_ip_address()
log_to_web(f"✅ Determined server IP for WebSocket: {SERVER_IP}")

def safe_value(value, fallback=0.0):
    if isinstance(value, bytes):
        try: value = value.decode('utf-8')
        except: return fallback
    try: return float(value)
    except (ValueError, TypeError): return value if isinstance(value, str) else fallback

# =========================
# WEBSOCKET PUSH SERVER
# =========================

async def send_to_clients(message_json):
    if websocket_clients:
        clients_copy = websocket_clients.copy()
        tasks = [client.send(message_json) for client in clients_copy]
        await asyncio.gather(*tasks, return_exceptions=True)

async def websocket_server_logic(websocket):
    global websocket_clients
    try:
        websocket_clients.add(websocket)
        await websocket.wait_closed()
    finally:
        websocket_clients.remove(websocket)

async def websocket_broadcast_loop():
    server = await websockets.serve(websocket_server_logic, "0.0.0.0", WEBSOCKET_PORT)
    log_to_web(f"✅ WebSocket push server running on ws://0.0.0.0:{WEBSOCKET_PORT}")
    loop = asyncio.get_running_loop()
    while True:
        try:
            message_dict = await loop.run_in_executor(None, update_queue.get)
            if message_dict:
                message_json = json.dumps(message_dict)
                await send_to_clients(message_json)
        except Exception:
            await asyncio.sleep(1.0)

def start_websocket_server_thread():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(websocket_broadcast_loop())

# =========================
# JOB MANAGEMENT
# =========================
def _check_active_jobs():
    global active_jobs
    conn = None
    try:
        jobs_to_complete = []
        with job_lock:
            if not active_jobs: return 
            now = datetime.now()
            for gateway_id, job_info in list(active_jobs.items()):
                if job_info['end_time'] <= now:
                    jobs_to_complete.append((job_info['job_id'], gateway_id))
        
        if not jobs_to_complete: return

        conn = get_db_connection()
        for job_id, gateway_id in jobs_to_complete:
            conn.execute("UPDATE recording_jobs SET status='completed' WHERE job_id=?", (job_id,))
            with job_lock:
                if gateway_id in active_jobs and active_jobs[gateway_id]['job_id'] == job_id:
                    del active_jobs[gateway_id]
            log_to_web(f"✅ Recording job {job_id} for {gateway_id} completed.")
            update_queue.put({"type": "job_status_update", "gateway_id": gateway_id})
        conn.commit()
    except Exception as e:
        log_to_web(f"❌ Error in _check_active_jobs: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

def job_management_thread_func():
    log_to_web("Scheduled job management thread started. First check in 60s.")
    while True:
        time.sleep(60) 
        try: _check_active_jobs()
        except Exception as e: log_to_web(f"❌ Error in job management: {e}")

# =========================
# MQTT CALLBACKS
# =========================
def on_connect(client, userdata, flags, rc):
    global mqtt_connection_status, mqtt_status_lock
    if rc == 0:
        log_to_web(f"✅ Connected to MQTT broker at {BROKER}:{PORT}")
        client.subscribe(TOPIC)
        with mqtt_status_lock:
            mqtt_connection_status = {"status": "connected", "message": f"Connected to {BROKER}:{PORT}"}
        update_queue.put({"type": "mqtt_status_update"})
    else:
        log_to_web(f"❌ MQTT connection failed with code: {rc}")
        with mqtt_status_lock:
            mqtt_connection_status = {"status": "failed", "message": f"Connection failed. Code: {rc}"}
        update_queue.put({"type": "mqtt_status_update"})

def on_disconnect(client, userdata, rc):
    global mqtt_connection_status, mqtt_status_lock
    if rc != 0:
        log_to_web(f"⚠️ MQTT unexpectedly disconnected with code: {rc}. Reconnecting...")
    with mqtt_status_lock:
        mqtt_connection_status = {"status": "disconnected", "message": "Disconnected. Reconnecting..."}
    update_queue.put({"type": "mqtt_status_update"})

def on_message(client, userdata, msg):
    # FIX 3: Robust Connection Handling (Reconnection on Error)
    # Don't rely on 'userdata' as it might be stale. Use a new local connection if needed.
    # For performance, we try to maintain a connection, but handle failures gracefully.
    
    # We will use a local logic: Open -> Write -> Close (safest for SQLite concurrency + WAL)
    # This completely eliminates "stale connection" errors.
    
    conn = None
    try:
        data_peek = json.loads(msg.payload.decode("utf-8"))
        gateway_id = data_peek.get("gateway_id")
        
        # Quick filter
        if not gateway_id: return
        
        sensor_id = data_peek.get("sensor_id", "unknown_0")
        channel = sensor_id.split('_')[-1]
        if channel not in ['1', '2', '3', '4']: return

        # Establish Fresh Connection for Write (Safest Pattern for Multithreaded SQLite)
        conn = get_db_connection()
        
        timestamp_ms = data_peek.get("timestamp", 0)
        timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000)
        timestamp_str = timestamp_dt.strftime('%Y-%m-%d %H:%M:%S') if timestamp_ms else "N/A"
        
        update_summary = None
        is_new_device = False
        
        with job_lock:
            is_recording = gateway_id in active_jobs

        with data_lock:
            if gateway_id not in data_storage:
                log_to_web(f"ℹ️ New device detected: {gateway_id}")
                data_storage[gateway_id] = {
                    "channels": {}, "rpm": 0.0, "last_seen": "N/A",
                    "rpm_trend": deque(maxlen=MAX_TREND_POINTS),
                    "rpm_trend_times": deque(maxlen=MAX_TREND_POINTS)
                }
                is_new_device = True
            
            if channel not in data_storage[gateway_id]["channels"]:
                data_storage[gateway_id]["channels"][channel] = {
                    "trends": {m: deque(maxlen=MAX_TREND_POINTS) for m in TRENDED_METRICS},
                    "trend_times": deque(maxlen=MAX_TREND_POINTS), "metrics": {}, "timestamp": "N/A",
                    "waveform_data": [], "spectrum_data": [], "sampling_freq": 12800,
                    "bandwidth": 5000, "lines": 6400
                }
            
            ch_data_ref = data_storage[gateway_id]["channels"][channel]
            
            # ... (Data Processing Logic Same as Before) ...
            if "Time waveform" in data_peek:
                pd = data_peek.get("process_data", {})
                waveform_raw = np.nan_to_num(data_peek.get("Time waveform", []))
                
                metrics = {
                    "Acceleration": np.nan_to_num(pd.get("Acceleration", {}).get("value", 0)),
                    "Velocity": np.nan_to_num(pd.get("Velocity", {}).get("value", 0)),
                    "Displacement": np.nan_to_num(pd.get("Displacement", {}).get("value", 0)),
                    "Crest Factor": np.nan_to_num(pd.get("CrestFactor", {}).get("value", 0)),
                    "True Peak": np.nan_to_num(pd.get("TruePeak", {}).get("value", 0)),
                    "Std Deviation": np.nan_to_num(np.std(waveform_raw)) if waveform_raw.size else 0
                }
                rpm_val = pd.get("RPM", 0)

                # Update InMemory Storage
                ch_data_ref["metrics"] = metrics
                ch_data_ref["timestamp"] = timestamp_str
                ch_data_ref["sensitivity"] = pd.get("SensorSensitivity", '---')
                
                for key, val in metrics.items():
                    if key in ch_data_ref["trends"]: ch_data_ref["trends"][key].append(val)
                ch_data_ref["trend_times"].append(timestamp_str)
                
                data_storage[gateway_id]["rpm"] = rpm_val
                data_storage[gateway_id]["last_seen"] = timestamp_str
                data_storage[gateway_id]["rpm_trend"].append(rpm_val)
                data_storage[gateway_id]["rpm_trend_times"].append(timestamp_str)

                fs = 0
                if waveform_raw.size > 0:
                    fs = data_peek.get("sampling_frequency", 12800)
                    ch_data_ref["waveform_data"] = waveform_raw.tolist()
                    ch_data_ref["sampling_freq"] = fs
                
                # DB Operations
                db_trends_to_write_latest = []
                db_trends_to_write_history = []
                
                for metric, value in metrics.items():
                    db_trends_to_write_latest.append((gateway_id, channel, metric, value, rpm_val, timestamp_dt))
                    if is_recording:
                        db_trends_to_write_history.append((timestamp_dt, gateway_id, channel, metric, value, rpm_val))
                
                conn.executemany("INSERT OR REPLACE INTO trends_latest (gateway_id, channel, metric_name, value, rpm, timestamp) VALUES (?, ?, ?, ?, ?, ?)", db_trends_to_write_latest)
                
                if db_trends_to_write_history:
                    conn.executemany("INSERT OR IGNORE INTO trends (timestamp, gateway_id, channel, metric_name, value, rpm) VALUES (?, ?, ?, ?, ?, ?)", db_trends_to_write_history)
                
                if waveform_raw.size > 0 and is_recording:
                    conn.execute("INSERT OR IGNORE INTO raw_waveforms (gateway_id, channel, timestamp, waveform_data, sampling_freq) VALUES (?, ?, ?, ?, ?)", 
                                 (gateway_id, channel, timestamp_dt, waveform_raw.tobytes(), fs))
                
                update_summary = {"type": "data_update", "gateway_id": gateway_id, "channel": channel}

            elif "Power spectrum" in data_peek:
                spectrum_raw = np.nan_to_num(data_peek.get("Power spectrum", []))
                if timestamp_str != "N/A": data_storage[gateway_id]["last_seen"] = timestamp_str
                
                ch_data_ref["spectrum_data"] = spectrum_raw.tolist()
                ch_data_ref["bandwidth"] = data_peek.get("bandwidth", 5000)
                ch_data_ref["lines"] = data_peek.get("lines", 6400)
                
                if is_recording and spectrum_raw.size > 0:
                    conn.execute("INSERT OR IGNORE INTO raw_spectrum (gateway_id, channel, timestamp, spectrum_data, bandwidth, lines) VALUES (?, ?, ?, ?, ?, ?)",
                                 (gateway_id, channel, timestamp_dt, spectrum_raw.tobytes(), ch_data_ref["bandwidth"], ch_data_ref["lines"]))
                
                update_summary = {"type": "data_update", "gateway_id": gateway_id, "channel": channel}

        if is_new_device:
            save_device_config_db(gateway_id, {})
        
        conn.commit()
        if update_summary: update_queue.put(update_summary)

    except Exception as e:
        log_to_web(f"❌ Error in MQTT Message Handler: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

def mqtt_thread_func():
    while True:
        try:
            # Note: We are NO LONGER passing a persistent DB connection.
            # on_message creates its own short-lived connection for robustness.
            client = mqtt.Client()
            if USERNAME: client.username_pw_set(USERNAME, PASSWORD)
            client.on_connect = on_connect
            client.on_message = on_message
            client.on_disconnect = on_disconnect 

            if PROTOCOL == "mqtts://":
                try:
                    client.tls_set(tls_version=ssl.PROTOCOL_TLS)
                except Exception as e:
                    log_to_web(f"❌ Error setting TLS: {e}")
                    time.sleep(5)
                    continue

            client.connect(BROKER, PORT, 60)
            client.loop_forever()
        except Exception as e:
            log_to_web(f"❌ MQTT client error: {e}. Retrying in 5s...")
            time.sleep(5)

# =========================
# MODBUS SERVER THREADS
# =========================

def modbus_updater_thread_func():
    global modbus_server_context, data_storage, data_lock
    log_to_web("ℹ️ Modbus updater thread started.")
    
    while True:
        try:
            time.sleep(2) 
            if modbus_server_context is None: continue 
            
            config = load_modbus_config()
            if not config.get("enabled", False): continue 

            registers_map = config.get("registers", [])
            if not registers_map: continue

            with data_lock:
                live_data = copy.deepcopy(data_storage)

            for i, reg_info in enumerate(registers_map):
                sensor_id = reg_info.get("sensor")
                channel_id = reg_info.get("channel")
                field_name = reg_info.get("field")
                value_float = 0.0
                
                try:
                    if sensor_id in live_data:
                        if field_name == "RPM":
                            rpm_val = live_data[sensor_id].get("rpm")
                            if rpm_val is not None: value_float = float(rpm_val)
                        elif channel_id in live_data[sensor_id].get("channels", {}):
                            metric_val = live_data[sensor_id]["channels"][channel_id].get("metrics", {}).get(field_name)
                            if metric_val is not None: value_float = float(metric_val)
                except Exception: pass 

                # FIX 1: Modbus Expansion Crash Protection
                try:
                    packed_float = struct.pack('>f', value_float)
                    registers_16bit = list(struct.unpack('>HH', packed_float))
                    modbus_address = i * 2 
                    # Check if address is within range of initialized block
                    # If user added registers but didn't restart, this write will fail.
                    # We catch it to prevent thread crash.
                    modbus_server_context[0].setValues(3, modbus_address, registers_16bit)
                except (IndexError, ValueError) as e:
                    # Silently ignore or log once per cycle if needed
                    # This happens when config has more regs than allocated memory
                    pass
                except Exception as e:
                    pass

        except Exception as e:
            log_to_web(f"❌ Modbus updater error: {e}")
            time.sleep(10)

def modbus_server_thread_func():
    global modbus_server_context
    config = load_modbus_config()
    if not config.get("enabled", False):
        log_to_web("ℹ️ Modbus server is disabled in config.")
        return

    try:
        registers_map = config.get("registers", [])
        num_registers = len(registers_map) * 2
        
        if num_registers == 0: num_registers = 2
        log_to_web(f"ℹ️ Initializing Modbus datastore with {num_registers} registers.")

        block = ModbusSequentialDataBlock(0, [0] * num_registers)
        slave_context = ModbusSlaveContext(hr=block, zero_mode=True) 
        modbus_server_context = ModbusServerContext(slaves=slave_context, single=True)

        identity = ModbusDeviceIdentification()
        identity.VendorName = 'Wilcoxon'
        identity.ProductCode = 'VibMon'
        identity.ProductName = 'Vibration Gateway'
        identity.ModelName = 'GW-001'

        log_to_web("✅ Modbus TCP server starting on 0.0.0.0:502...")
        StartTcpServer(
            context=modbus_server_context,
            identity=identity,
            address=("0.0.0.0", 502) 
        )
        
    except Exception as e:
        log_to_web(f"❌ Modbus server failed to start: {e}. Check if port 502 is in use.")

# =========================
# OPC UA SERVER THREAD
# =========================

async def run_opcua_server():
    global opcua_server, opcua_node_map, opcua_running
    
    opcua_server = Server()
    await opcua_server.init()
    opcua_server.set_endpoint(f"opc.tcp://0.0.0.0:{OPCUA_PORT}")
    opcua_server.set_server_name("Wilcoxon Vibration Server")
    
    # Register Namespace
    idx = await opcua_server.register_namespace("http://wilcoxon.com/vibmon")
    
    # Start the server
    log_to_web(f"✅ OPC UA Server starting on opc.tcp://0.0.0.0:{OPCUA_PORT}")
    opcua_running = True
    
    async with opcua_server:
        while True:
            try:
                # 1. Get Live Data
                with data_lock:
                    live_data = copy.deepcopy(data_storage)
                
                # 2. Iterate and update/create nodes
                objects_node = opcua_server.nodes.objects
                
                for gateway_id, device_data in live_data.items():
                    # A. Gateway Node
                    if gateway_id not in opcua_node_map:
                        g_node = await objects_node.add_object(idx, f"Device_{gateway_id}")
                        opcua_node_map[gateway_id] = {"node": g_node, "channels": {}, "rpm_node": None}
                        log_to_web(f"➕ OPC UA: Added Device {gateway_id}")
                    
                    g_map = opcua_node_map[gateway_id]
                    
                    # B. RPM
                    rpm_val = device_data.get("rpm", 0.0)
                    if g_map["rpm_node"] is None:
                        rpm_node = await g_map["node"].add_variable(idx, "RPM", rpm_val)
                        await rpm_node.set_writable() # Optional
                        g_map["rpm_node"] = rpm_node
                    else:
                        await g_map["rpm_node"].write_value(float(rpm_val))
                        
                    # C. Channels
                    for ch in ['1', '2', '3', '4']:
                        ch_data = device_data.get("channels", {}).get(ch, {})
                        if not ch_data: continue
                        
                        metrics = ch_data.get("metrics", {})
                        if not metrics: continue
                        
                        if ch not in g_map["channels"]:
                            ch_node = await g_map["node"].add_object(idx, f"Channel_{ch}")
                            g_map["channels"][ch] = {"node": ch_node, "metrics": {}}
                        
                        c_map = g_map["channels"][ch]
                        
                        # D. Metrics
                        for metric_name, val in metrics.items():
                            clean_name = metric_name.replace(" ", "_")
                            if clean_name not in c_map["metrics"]:
                                m_node = await c_map["node"].add_variable(idx, clean_name, float(val))
                                c_map["metrics"][clean_name] = m_node
                            else:
                                await c_map["metrics"][clean_name].write_value(float(val))

            except Exception as e:
                log_to_web(f"❌ OPC UA Loop Error: {e}")
            
            await asyncio.sleep(1) # Update every 1 second

def opcua_thread_entry():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(run_opcua_server())
    except Exception as e:
        log_to_web(f"❌ OPC UA Server Thread Crashed: {e}")

# =========================
# DASH APP CONFIG & LAYOUT
# =========================
app = Dash(__name__, 
            external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME], 
            suppress_callback_exceptions=True,
            assets_folder='assets') 

app.title = "Wilcoxon Vibration Monitor"
led_base_style = {"width": "20px","height": "20px","borderRadius": "50%","display": "inline-block","marginRight": "10px","verticalAlign": "middle"}
led_green_style = {"backgroundColor": "#22c55e", **led_base_style}
led_red_style = {"backgroundColor": "#ef4444", **led_base_style}

@app.server.route('/download/<filename>')
def download_data_file(filename):
    if filename != os.path.basename(DB_FILE): return "File not found.", 404
    file_path = DB_FILE
    if not os.path.exists(file_path): return "File not found.", 404
    return send_file(file_path, mimetype='application/x-sqlite3', as_attachment=True, download_name=filename)

def create_channel_box(channel_id, gateway_id, data, device_config):
    ch = str(channel_id)
    base_3d = {"height": "100%", "backgroundColor": "#ffffff", "borderRadius": "8px", "boxShadow": "0 4px 12px rgba(0,0,0,0.1)", "border": "none"}
    active_style = {**base_3d, "borderTop": "4px solid #198754"}
    inactive_style = {**base_3d, "borderTop": "4px solid #dc3545"}

    trend_config = device_config.get("trends", {})
    label_config = device_config.get("labels", {})
    alarm_config = device_config.get("alarms", {}).get(ch, {})

    is_checked = trend_config.get(ch, False)
    current_label = label_config.get(ch, "")
    is_alarm_enabled = alarm_config.get("enabled", False)

    sens, accel, vel, disp, crest, peak, stddev, ts = "---", "---", "---", "---", "---", "---", "---", "N/A"
    vel_val = 0.0 

    is_stale = True
    if data and data.get("timestamp") and data["timestamp"] != "N/A":
        ts = data["timestamp"] 
        try:
            last_ts = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
            if (datetime.now() - last_ts) < timedelta(minutes=5):
                is_stale = False
        except ValueError: pass 

    if is_stale:
        style, led_style, led_class = inactive_style, led_red_style, ""
    else:
        style, led_style, led_class = active_style, {**led_green_style}, "led-green-pulsing"

    if data and data.get("metrics"):
        metrics = data.get("metrics", {})
        raw_sens = data.get('sensitivity', '---')
        sens = f"{safe_value(raw_sens, fallback='---')} mV/g"
        
        accel_val = safe_value(metrics.get('Acceleration', 0.0))
        vel_val = safe_value(metrics.get('Velocity', 0.0)) 
        disp_val = safe_value(metrics.get('Displacement', 0.0))
        crest_val = safe_value(metrics.get('Crest Factor', 0.0))
        peak_val = safe_value(metrics.get('True Peak', 0.0))
        stddev_val = safe_value(metrics.get('Std Deviation', 0.0))

        accel = f"{accel_val:.4f} g"
        vel = f"{vel_val:.4f} mm/s"
        disp = f"{disp_val:.4f} µm"
        crest = f"{crest_val:.4f}"
        peak = f"{peak_val:.4f} g"
        stddev = f"{stddev_val:.4f}"

    if is_alarm_enabled:
        vel_alert = alarm_config.get("vel_alert", 0.0)
        vel_warn = alarm_config.get("vel_warn", 0.0)

        if vel_alert > 0 and isinstance(vel_val, (int, float)) and vel_val >= vel_alert:
            style = {**style, "borderTop": "4px solid #dc3545", "animation": "pulse-red 1.5s infinite"}
        elif vel_warn > 0 and isinstance(vel_val, (int, float)) and vel_val >= vel_warn:
            style = {**style, "borderTop": "4px solid #ffc107", "animation": "pulse-yellow 1.5s infinite"}

    wf_btn_id = {"type": "ch-wf-btn", "gateway": gateway_id, "channel": ch}
    spec_btn_id = {"type": "ch-spec-btn", "gateway": gateway_id, "channel": ch}
    trend_check_id = {"type": "ch-trend-check", "gateway": gateway_id, "channel": ch}
    alarm_btn_id = {"type": "ch-alarm-btn", "gateway": gateway_id, "channel": ch}
    label_input_id = {"type": "ch-label-input", "gateway": gateway_id, "channel": ch}

    alarm_indicator = html.I(className="fas fa-bell ms-2 text-warning", style={"fontSize": "0.8em", "verticalAlign": "middle"}) if is_alarm_enabled else ""

    label_input_div = html.Div([
        html.I(className="fas fa-pencil-alt me-2", style={"fontSize": "0.8em", "color": "#6c757d", "verticalAlign": "middle"}),
        dcc.Input(
            id=label_input_id, value=current_label, placeholder="Enter label",
            maxLength=50, debounce=True,
            style={'width': 'calc(100% - 30px)', 'border': 'none', 'borderBottom': '1px dashed #ccc', 'fontSize': '0.9em', 'verticalAlign': 'middle', 'backgroundColor': 'transparent'}
        )
    ], className="mt-2", style={"paddingLeft": "30px"})

    card_header = dbc.CardHeader([
        dbc.Row([
            dbc.Col([
                html.Span(style=led_style, className=led_class),
                html.Strong(f"CH{ch}", style={"verticalAlign": "middle"}),
                alarm_indicator
            ], width="auto", className="d-flex align-items-center"), 
            dbc.Col([
                dbc.Checkbox(id=trend_check_id, label="Trend", value=is_checked, className="me-2"), 
                dbc.Button(html.I(className="fas fa-cog"), id=alarm_btn_id, color="secondary", outline=True, size="sm")
            ], width="auto", className="ms-auto d-flex align-items-center") 
        ], align="center", justify="between"), 
        label_input_div
    ], style={"backgroundColor": "#f8f9fa","borderBottom": "1px solid #dee2e6", "padding": "0.75rem 1rem"})

    card_body_content = [
        html.P([html.Strong("Coupling: "), html.Span("IEPE")]),
        html.P([html.Strong("Sensitivity: "), html.Span(sens)]),
        html.P([html.Strong("Acceleration: "), html.Span(accel)]),
        html.P([html.Strong("Velocity: "), html.Span(vel)]),
        html.P([html.Strong("Displacement: "), html.Span(disp)]),
        html.P([html.Strong("Crest Factor: "), html.Span(crest)]),
        html.P([html.Strong("True Peak: "), html.Span(peak)]),
        html.P([html.Strong("Std Deviation: "), html.Span(stddev)]),
        html.P([html.Strong("Timestamp: "), html.Span(ts)], className="mt-3 small text-muted"),
        dbc.ButtonGroup([
            html.Button("Waveform", id=wf_btn_id, className="btn btn-outline-primary btn-sm"),
            html.Button("Spectrum", id=spec_btn_id, className="btn btn-outline-primary btn-sm")
        ], className="w-100 mt-2")
    ]

    return dbc.Col(dbc.Card([card_header, dbc.CardBody(card_body_content, style={"padding": "1rem"})], style=style), lg=2, md=4, sm=6, xs=12)

def create_rpm_box(gateway_id, rpm_val, any_channel_active, device_config):
    base_3d = {"height": "100%", "backgroundColor": "#ffffff", "borderRadius": "8px", "boxShadow": "0 4px 12px rgba(0,0,0,0.1)", "border": "none"}
    active_style = {**base_3d, "borderTop": "4px solid #198754"}
    inactive_style = {**base_3d, "borderTop": "4px solid #dc3545"}
    
    trend_config = device_config.get("trends", {})
    is_checked = trend_config.get("RPM", False)

    if not any_channel_active: 
        style, led_style, led_class = inactive_style, led_red_style, ""
    else: 
        style, led_style, led_class = active_style, {**led_green_style}, "led-green-pulsing"
    
    val = f"{rpm_val:.1f}" if isinstance(rpm_val, (int, float)) else "---" 
    trend_check_id = {"type": "rpm-trend-check", "gateway": gateway_id}
    
    return dbc.Col(dbc.Card([dbc.CardHeader([html.Span(style=led_style, className=led_class),html.Strong("RPM", style={"verticalAlign": "middle"}),dbc.Checkbox(id=trend_check_id, label="Trend", value=is_checked, className="float-end")], style={"backgroundColor": "#f8f9fa","borderBottom": "1px solid #dee2e6"}),dbc.CardBody(html.H4(val, className="text-center my-4 py-4"), style={"padding": "1rem"})], style=style), lg=2, md=4, sm=6, xs=12)

header = dbc.Navbar(
    dbc.Container(
        [
            dbc.Button(html.I(className="fas fa-bars"), id="sidebar-toggle-btn", color="light", outline=True, n_clicks=0, className="me-2"),
            html.A("Real-Time Vibration Monitor", href="#", style={"textDecoration": "none", "color": "white", "fontSize": "1.25rem"})
        ], fluid=True
    ), color="dark", dark=True, fixed="top" 
)

sidebar = html.Div([
    html.H4("Menu", className="text-white p-3"),
    dbc.Nav([
        dbc.NavLink([html.I(className="fas fa-chart-line me-2"), "Dashboard"], href="/", active="exact", className="text-white"),
        dbc.NavLink([html.I(className="fas fa-server me-2"), "Device Info"], href="/device", active="exact", className="text-white"),
        dbc.NavLink([html.I(className="fas fa-database me-2"), "Data Storage"], href="/data-storage", active="exact", className="text-white"),
        dbc.NavLink([html.I(className="fas fa-search me-2"), "History Explorer"], href="/history", active="exact", className="text-white"), 
        dbc.NavLink([html.I(className="fas fa-bug me-2"), "Debug Log"], href="/debug", active="exact", className="text-white"),
        dbc.NavLink([html.I(className="fas fa-broadcast-tower me-2"), "MQTT Settings"], href="/mqtt", active="exact", className="text-white"),
        dbc.NavLink([html.I(className="fas fa-network-wired me-2"), "Ethernet Settings"], href="/ethernet", active="exact", className="text-white"),
        dbc.NavLink([html.I(className="fas fa-rss me-2"), "Modbus TCP"], href="/modbus", active="exact", className="text-white"),
        dbc.NavLink([html.I(className="fas fa-project-diagram me-2"), "OPC UA Server"], href="/opcua", active="exact", className="text-white"), 
    ], vertical=True, pills=True)
], id="sidebar", style={"position": "fixed", "top": 0, "left": 0, "bottom": 0, "width": "18rem", "padding": "1rem", "paddingTop": "4rem", "backgroundColor": "#343a40", "transition": "all 0.3s"})

data_storage_layout = dcc.Loading(id="loading-data-storage", type="default", children=html.Div(id="data-storage-container"))

debug_layout = html.Div([
    dbc.Row([
        dbc.Col(html.H4("Live Server Log"), width="auto"),
        dbc.Col(dbc.Button([html.I(className="fas fa-trash-alt me-2"), "Clear Log"], id="clear-log-btn", color="danger", outline=True, size="sm"), width="auto", className="ms-auto")
    ], align="center", className="mb-4 mt-4"),
    dbc.Row(dbc.Col(dbc.Card(dbc.CardBody(html.Pre(id='debug-log-output', style={'maxHeight': '600px', 'overflowY': 'scroll', 'backgroundColor': '#f8f9fa', 'border': '1px solid #dee2e6', 'padding': '10px', 'borderRadius': '5px'})))))
])

def create_mqtt_layout():
    current_config = load_mqtt_config()
    layout = html.Div([
        dbc.Row(dbc.Col(html.H4("MQTT Settings"), className="mb-4 mt-4")),
        dbc.Row(dbc.Col(dbc.Alert(id="mqtt-status-bar", children="Checking connection status...", color="secondary", className="mb-3"))),
        dbc.Row(dbc.Col(dbc.Card([dbc.CardHeader("MQTT Configuration"), dbc.CardBody([
            dbc.Alert([html.I(className="fas fa-exclamation-triangle me-2"), "Changes saved here will only apply after ", html.Strong("restarting the application.")], color="warning", className="mb-3"),
            dbc.Alert([html.I(className="fas fa-info-circle me-2"), "Default port for mqtt:// is 1883. ", html.Br(), "Default port for mqtts:// is 8883."], color="info", className="mb-3"),
            dbc.Form([
                dbc.Row([dbc.Label("Host", width=2), dbc.Col(dbc.InputGroup([dbc.Select(id="mqtt-protocol-select", options=[{"label": "mqtt://", "value": "mqtt://"}, {"label": "mqtts://", "value": "mqtts://"}], value=current_config.get("PROTOCOL", "mqtt://")), dbc.Input(id="mqtt-host-input", value=current_config.get("BROKER", ""), placeholder="e.g., 213.111.157.130")]), width=10)], className="mb-3"),
                dbc.Row([dbc.Label("Port", width=2), dbc.Col(dbc.Input(id="mqtt-port-input", value=current_config.get("PORT", 1883), type="number"), width=10)], className="mb-3"),
                dbc.Row([dbc.Label("Username", width=2), dbc.Col(dbc.Input(id="mqtt-user-input", value=current_config.get("USERNAME", ""), placeholder="Leave blank if none"), width=10)], className="mb-3"),
                dbc.Row([dbc.Label("Password", width=2), dbc.Col(dbc.Input(id="mqtt-pass-input", value=current_config.get("PASSWORD", ""), type="password", placeholder="Leave blank if none"), width=10)], className="mb-3"),
                dbc.Row([dbc.Label("Topic", width=2), dbc.Col(dbc.Input(id="mqtt-topic-input", value=current_config.get("TOPIC", ""), placeholder="e.g., wilcoxon"), width=10)], className="mb-3"),
            ]),
            html.Hr(),
            dbc.Alert(id="mqtt-test-status", is_open=False, duration=5000),
            dbc.Row([dbc.Col(dbc.Button("Save", id="save-mqtt-btn", color="primary", n_clicks=0, className="w-100"), md=6), dbc.Col(dbc.Button("Test Connection", id="test-mqtt-btn", color="info", outline=True, n_clicks=0, className="w-100"), md=6)], className="mb-3"),
            dbc.Alert(id="mqtt-save-status", children="Settings saved. Please restart the application.", color="success", is_open=False, duration=5000, className="mt-3"),
            html.Hr(className="my-4"),
            dbc.Button([html.I(className="fas fa-power-off me-2"), "Restart Application"], id={'type': 'restart-app-btn', 'page': 'mqtt'}, color="danger", outline=True, className="w-100")
        ])]), lg=8)),
        dcc.Interval(id="mqtt-status-interval", interval=5000, n_intervals=0)
    ])
    return layout

ethernet_layout = html.Div([
    dbc.Row(dbc.Col(html.H4("Ethernet Settings (eth0)"), className="mb-4 mt-4")),
    dbc.Row(dbc.Col(dbc.Card([dbc.CardHeader("Current Network Configuration"), dcc.Loading(id="loading-ethernet", type="default", children=dbc.CardBody(id="ethernet-settings-card-body"))]), lg=6))
])

# =========================
# MODBUS LAYOUT
# =========================
def generate_modbus_table(registers_list, live_data_storage):
    header = html.Thead(html.Tr([html.Th("Register"), html.Th("Sensor"), html.Th("Channel"), html.Th("Data type"), html.Th("Field"), html.Th("Current Value"), html.Th("Actions", style={"textAlign": "center"})]))
    body_rows = []
    if not registers_list:
        body_rows.append(html.Tr(html.Td("No registers configured.", colSpan=7, className="text-center text-muted")))
    else:
        for i, reg in enumerate(registers_list):
            action_buttons = dbc.ButtonGroup([
                dbc.Button(html.I(className="fas fa-arrow-up"), id={'type': 'modbus-move-up-btn', 'index': i}, color="secondary", outline=True, size="sm", disabled=(i == 0)),
                dbc.Button(html.I(className="fas fa-arrow-down"), id={'type': 'modbus-move-down-btn', 'index': i}, color="secondary", outline=True, size="sm", disabled=(i == len(registers_list) - 1)),
                dbc.Button(html.I(className="fas fa-trash-alt"), id={'type': 'modbus-delete-btn', 'index': i}, color="danger", outline=True, size="sm", className="ms-2")
            ])
            sensor_id = reg.get("sensor", "N/A"); channel_id = reg.get("channel", "N/A"); field_name = reg.get("field", "N/A")
            current_value_str = "N/A"
            try:
                if sensor_id in live_data_storage:
                    if field_name == "RPM":
                        rpm_val = live_data_storage[sensor_id].get("rpm")
                        if rpm_val is not None: current_value_str = f"{rpm_val:.2f}"
                    elif channel_id in live_data_storage[sensor_id].get("channels", {}):
                        metric_val = live_data_storage[sensor_id]["channels"][channel_id].get("metrics", {}).get(field_name)
                        if metric_val is not None: current_value_str = f"{metric_val:.4f}"
            except Exception: current_value_str = "Error"
            body_rows.append(html.Tr([html.Td(i), html.Td(sensor_id), html.Td(channel_id), html.Td(reg.get("data_type", "N/A")), html.Td(field_name), html.Td(current_value_str, style={'fontWeight': 'bold', 'color': 'black'}), html.Td(action_buttons, style={"textAlign": "center"})]))
    return dbc.Table([header, html.Tbody(body_rows)], bordered=True, hover=True, striped=True)

def create_modbus_layout():
    current_config = load_modbus_config()
    current_registers = current_config.get("registers", [])
    is_enabled = current_config.get("enabled", False)
    
    # NEW DIAGNOSTICS LOGIC
    status_text = "Running" if is_enabled else "Stopped"
    status_color = "success" if is_enabled else "danger"
    status_icon = "fas fa-check-circle fa-2x text-success" if is_enabled else "fas fa-stop-circle fa-2x text-danger"

    gateway_options = []
    try:
        conn = get_db_connection()
        cursor = conn.execute("SELECT gateway_id FROM config ORDER BY gateway_id")
        gateway_options = [{'label': row[0], 'value': row[0]} for row in cursor.fetchall()]
        conn.close()
    except Exception: pass
    value_options = [{'label': m, 'value': m} for m in TRENDED_METRICS] + [{'label': 'RPM', 'value': 'RPM'}]
    
    layout = html.Div([
        dbc.Row(dbc.Col(html.H4("Modbus TCP Server"), className="mb-4 mt-4")),
        
        # --- NEW DIAGNOSTICS CARD ---
        dbc.Row([
            dbc.Col(dbc.Card([
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.Div(html.I(className=status_icon), className="mb-2"),
                            html.H5(status_text, className=f"text-{status_color}")
                        ], width=4, className="text-center border-end"),
                        dbc.Col([
                            html.H2(id="modbus-client-count", children="-", className="text-primary fw-bold"),
                            html.Small("Active Connections", className="text-muted text-uppercase")
                        ], width=4, className="text-center border-end"),
                        dbc.Col([
                            html.H2(str(len(current_registers)), className="text-dark fw-bold"),
                            html.Small("Registers Mapped", className="text-muted text-uppercase")
                        ], width=4, className="text-center")
                    ])
                ])
            ], className="mb-4 shadow-sm"), lg=12)
        ]),
        
        dbc.Card(dbc.CardBody([
            dbc.Alert(id="modbus-save-status", is_open=False, duration=4000),
            dbc.Alert("Changes saved here may require an application restart to take effect.", color="info", className="mb-3"),
            dbc.Switch(id="modbus-enable-switch", label="Enable Modbus TCP Server", value=is_enabled, className="mb-3 fs-5"),
            html.P("Connect to the Gateway's Modbus server on port 502. Slave ID is always 1."),
            html.Hr(),
            dbc.Row([
                dbc.Col([dbc.Label("Select a sensor (Gateway)"), dcc.Dropdown(id='modbus-sensor-select', options=gateway_options, placeholder="Select a sensor...")], md=4),
                dbc.Col([dbc.Label("Sensor Channel (if not RPM)"), dcc.Dropdown(id='modbus-channel-select', options=[{'label': f'Channel {i}', 'value': str(i)} for i in range(1, 5)], placeholder="Select channel...")], md=4),
                dbc.Col([dbc.Label("Sensor value (Field)"), dcc.Dropdown(id='modbus-value-select', options=value_options, placeholder="Select a value...")], md=4)
            ], className="mb-3"),
            dbc.Button("ADD", id="modbus-add-btn", n_clicks=0, color="primary", className="mb-3"),
            html.Hr(),
            html.H5("Register Map"),
            dcc.Loading(id="loading-modbus-table", type="default", children=html.Div(id='modbus-table-container', children=generate_modbus_table(current_registers, {}))),
            html.Hr(className="my-4"),
            dbc.Button([html.I(className="fas fa-power-off me-2"), "Restart Application"], id={'type': 'restart-app-btn', 'page': 'modbus'}, color="danger", outline=True, className="w-100")
        ]))
    ])
    return layout

# =========================
# OPC UA LAYOUT (PRO VERSION)
# =========================
def create_opcua_layout():
    status_text = "Running" if opcua_enabled_flag else "Stopped"
    status_color = "success" if opcua_enabled_flag else "danger"
    status_icon = "fas fa-check-circle fa-3x text-success" if opcua_enabled_flag else "fas fa-stop-circle fa-3x text-danger"
    
    # Calculate Uptime
    uptime_str = "0:00:00"
    if opcua_enabled_flag:
        delta = datetime.now() - OPCUA_START_TIME
        uptime_str = str(delta).split('.')[0]

    return html.Div([
        # Header
        dbc.Row(dbc.Col([
            html.H3(["OPC UA Server Manager", dbc.Badge("PRO", color="warning", className="text-dark ms-2", style={"fontSize": "0.5em", "verticalAlign": "middle"})]),
            html.P("Manage the embedded OPC UA server and browse the live address space.", className="text-muted")
        ], width=12), className="mb-4"),

        dbc.Row([
            # Server Control Card
            dbc.Col(dbc.Card([
                html.Div([
                    html.Span([html.I(className="fas fa-power-off me-2 text-primary"), "Server Control"]),
                    dbc.Switch(id="opc-server-switch", value=opcua_enabled_flag, className="form-check-input")
                ], className="opc-card-header d-flex justify-content-between align-items-center"),
                dbc.CardBody([
                    html.Div([
                        html.Div(html.I(id="opc-status-icon", className=status_icon), className="me-3"),
                        html.Div([
                            html.H5(["Status: ", html.Span(status_text, id="opc-status-text", className=f"fw-bold text-{status_color}")]),
                            html.Small(id="opc-uptime-display", children=f"Uptime: {uptime_str}", className="text-muted font-monospace")
                        ])
                    ], className="d-flex align-items-center mb-3"),
                    html.Hr(),
                    dbc.Label("Endpoint URL (TCP)", className="text-muted fw-bold small text-uppercase"),
                    dbc.InputGroup([
                        dbc.InputGroupText(html.I(className="fas fa-network-wired")),
                        dbc.Input(id="opc-endpoint-url", value=f"opc.tcp://{SERVER_IP}:{OPCUA_PORT}", readonly=True, className="font-monospace"),
                        dcc.Clipboard(target_id="opc-endpoint-url", title="Copy", style={"fontSize": 20, "display": "inline-block", "marginLeft": "10px", "marginTop": "5px", "color": "#6c757d"})
                    ], className="mb-2"),
                    html.Div([html.I(className="fas fa-info-circle me-1"), " Port 4840 is open. Anonymous login allowed."], className="small text-muted")
                ])
            ], className="opc-card h-100"), lg=6, className="mb-4"),

            # Diagnostics Card
            dbc.Col(dbc.Card([
                html.Div([html.I(className="fas fa-heartbeat me-2 text-info"), "Live Diagnostics"], className="opc-card-header"),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.H2(id="opc-client-count", children="0", className="text-primary fw-bold"),
                            html.Small("Active Clients", className="text-muted text-uppercase")
                        ], width=6, className="text-center border-end"),
                        dbc.Col([
                            html.H2(str(len(data_storage)), className="text-success fw-bold"),
                            html.Small("Monitored Devices", className="text-muted text-uppercase")
                        ], width=6, className="text-center")
                    ], className="py-2"),
                    html.Hr(),
                    dbc.ListGroup([
                        dbc.ListGroupItem([html.Strong("Security:"), html.Span("None (Anonymous)", className="float-end text-danger")]),
                        dbc.ListGroupItem([html.Strong("Max Sessions:"), html.Span("100", className="float-end")]),
                    ], flush=True, className="small")
                ])
            ], className="opc-card h-100"), lg=6, className="mb-4")
        ]),

        # Live Address Space
        dbc.Row(dbc.Col(dbc.Card([
            html.Div([
                html.Span([html.I(className="fas fa-sitemap me-2 text-purple"), "Live Address Space (Node Tree)"]),
                dbc.Badge("Live Updates Active", color="success", className="pulse-badge")
            ], className="opc-card-header d-flex justify-content-between align-items-center"),
            dbc.CardBody(
                html.Div(id="opc-live-tree-container", children="Waiting for data..."), 
                className="p-0"
            )
        ], className="opc-card"), width=12))
    ])

history_layout = html.Div([
    dbc.Row(dbc.Col(html.H4("Historical Data Explorer"), className="mb-4 mt-4")),
    dbc.Card(dbc.CardBody([
        dbc.Row([
            dbc.Col([dbc.Label("Select Date Range"), dcc.DatePickerRange(id='history-date-picker', min_date_allowed=datetime(2020, 1, 1), max_date_allowed=datetime.now() + timedelta(days=1), start_date=datetime.now().date() - timedelta(days=1), end_date=datetime.now().date() + timedelta(days=1), display_format='YYYY-MM-DD', className="w-100")], lg=3, md=6),
            dbc.Col([dbc.Label("Select Gateway"), dcc.Dropdown(id='history-gateway-select', placeholder="Select Gateway...", clearable=False)], lg=3, md=6),
            dbc.Col([dbc.Label("Select Channel"), dcc.Dropdown(id='history-channel-select', options=[{'label': f'Channel {i}', 'value': str(i)} for i in range(1, 5)] + [{'label': 'RPM', 'value': 'RPM'}], value='1', clearable=False)], lg=2, md=6),
            dbc.Col([dbc.Label("Select Metric"), dcc.Dropdown(id='history-metric-select', clearable=False)], lg=2, md=6),
            dbc.Col([dbc.Button([html.I(className="fas fa-search me-2"), "Load Data"], id="load-history-btn", color="primary", className="w-100", style={"marginTop": "32px"})], lg=2, md=12)
        ])
    ]), className="mb-4"),
    dbc.Card(dbc.CardBody([
        dbc.Row(dbc.Col(dbc.Button([html.I(className="fas fa-download me-2"), "Download as CSV"], id="download-history-csv-btn", color="success", outline=True, className="float-end", size="sm"))),
        dcc.Loading(id="loading-history-graph", type="default", children=dcc.Graph(id="history-graph", style={"height": "500px"})),
        dcc.Download(id="download-history-component")
    ]))
])

system_info_panel = dbc.Card(dbc.CardBody(dbc.ListGroup([dbc.ListGroupItem([html.I(className="fas fa-barcode me-2"), html.Strong("Serial Number: "), html.Span(id="pi-serial")]),dbc.ListGroupItem([html.I(className="fas fa-clock me-2"), html.Strong("Date/Time: "), html.Span(id="pi-datetime")]),dbc.ListGroupItem([html.I(className="fas fa-hourglass-start me-2"), html.Strong("Uptime: "), html.Span(id="pi-uptime")]),dbc.ListGroupItem([html.I(className="fas fa-memory me-2"), html.Strong("Total Memory / Free: "), html.Span(id="pi-memory")]),dbc.ListGroupItem([html.I(className="fas fa-temperature-high me-2"), html.Strong("System Temperature: "), html.Span(id="pi-temp")]),dbc.ListGroupItem([html.I(className="fas fa-hdd me-2"), html.Strong("Storage Total / Free: "), html.Span(id="pi-disk")]),dbc.ListGroupItem([html.I(className="fas fa-microchip me-2"), html.Strong("CPU Load: "), html.Span(id="pi-cpuload")]),dbc.ListGroupItem([html.I(className="fas fa-network-wired me-2"), html.Strong("Connection IP: "), html.Span(id="pi-ip")])], flush=True, className="small"), style={"padding": "0.5rem"}), className="mb-4", style={"boxShadow": "0 4px 12px rgba(0,0,0,0.1)","border": "none","borderRadius": "8px"})

def create_datetime_settings_card():
    timezone_options, current_tz, ntp_on = get_system_time_settings()
    return dbc.Card([
        dbc.CardHeader("Datetime Setting"),
        dbc.CardBody([
            dcc.Interval(id='time-update-interval', interval=1000, n_intervals=0),
            dbc.Row([dbc.Col(dbc.Label("Auto-set Time:"), width="auto"), dbc.Col(dbc.Switch(id='time-auto-toggle', value=ntp_on), width="auto")], align="center", className="mb-3"),
            dbc.Form([
                dbc.Row([dbc.Label("Current Time:", width=3), dbc.Col(dbc.InputGroup([dbc.Input(id="time-current-display", value="Loading...", readonly=True), dbc.Button("Sync", id="time-sync-btn", color="primary", outline=True, n_clicks=0)]), width=9)], className="mb-3"),
                dbc.Row([dbc.Label("NTP Server:", width=3), dbc.Col(dbc.Input(id="time-ntp-server-input", value="time.google.com", disabled=ntp_on), width=9)], className="mb-3"),
                dbc.Row([dbc.Label("Time Zone:", width=3), dbc.Col(dcc.Dropdown(id='time-zone-dropdown', options=timezone_options, value=current_tz, disabled=ntp_on), width=9)], className="mb-3"),
                dbc.Row(dbc.Col(dbc.Button("Auto Detect", id="time-autodetect-btn", color="secondary", outline=True, n_clicks=0, size="sm"), width={"size": "auto", "offset": 3}), className="mb-3"),
                html.Hr(),
                dbc.Button("Save", id="time-save-btn", color="primary", n_clicks=0, className="w-100"),
                dbc.Alert(id="time-save-status", color="info", is_open=False, duration=4000, className="mt-3"),
                html.Hr(className="my-4"),
                dbc.Button([html.I(className="fas fa-power-off me-2"), "Restart Application"], id={'type': 'restart-app-btn', 'page': 'datetime'}, color="danger", outline=True, className="w-100")
            ])
        ])
    ], style={"boxShadow": "0 4px 12px rgba(0,0,0,0.1)", "border": "none", "borderRadius": "8px"})

dashboard_layout = html.Div([dbc.Row(dbc.Col(system_info_panel)), html.Div(id="device-dashboard-container")])

device_info_layout = html.Div([
    dbc.Row(dbc.Col(html.H5("System Information")), className="mb-2 mt-4"),
    dbc.Row([dbc.Col(dbc.Card([dbc.CardHeader("Gateway Details"), dbc.CardBody([html.Div(id="device-info-list"), html.Hr(), html.P([html.Strong("MQTT Broker: "), html.Span(f"{BROKER}:{PORT}")]), html.P([html.Strong("MQTT Topic: "), html.Span(TOPIC)])])], style={"boxShadow": "0 4px 12px rgba(0,0,0,0.1)","border": "none","borderRadius": "8px"}), width=6)], className="mb-4"),
    dbc.Row([dbc.Col(create_datetime_settings_card(), width=6)])
])

content = html.Div(id="page-content", style={"marginLeft": "18rem", "padding": "2rem 1rem", "paddingTop": "5rem", "transition": "all 0.3s"})
graph_modal = dbc.Modal([dbc.ModalHeader(id="modal-header"),dbc.ModalBody([dcc.Graph(id="modal-graph", style={"height": "450px"}),html.Div(id="modal-peak-table-container", className="mt-3")]),dbc.ModalFooter(html.Button("Close", id="modal-close-btn", className="btn btn-primary ms-auto"))], id="graph-modal", is_open=False, size="xl")
alarm_modal = dbc.Modal([dbc.ModalHeader(id="alarm-modal-header"),dbc.ModalBody(id="alarm-modal-body"),dbc.ModalFooter([dbc.Button("Save", id="alarm-modal-save-btn", color="primary"),dbc.Button("Cancel", id="alarm-modal-close-btn", color="secondary")])], id="alarm-modal", is_open=False)
initial_device_config = load_data_from_db_on_startup()

app.layout = html.Div([
    dcc.Location(id="url"),
    dcc.Store(id='sidebar-state-store', storage_type='session', data=True),
    dcc.Store(id='device-config-store', storage_type='session', data=initial_device_config),
    dcc.Store(id='store-gateway-to-delete'), dcc.ConfirmDialog(id='confirm-delete-dialog'),
    dcc.Store(id='store-accordion-state', storage_type='session', data=[]),
    dcc.Store(id='store-file-to-delete'), dcc.Store(id='store-device-record-to-delete'),
    dcc.ConfirmDialog(id='confirm-file-delete-dialog', message=f'Are you sure you want to permanently delete the database file {os.path.basename(DB_FILE)}? This will DELETE ALL stored data.'),
    dcc.ConfirmDialog(id='confirm-device-record-delete-dialog', message='Are you sure you want to delete ALL historical records and configuration for this device?'),
    dcc.Store(id='store-alarm-config-data'), dcc.Download(id="download-excel-component"),
    dcc.ConfirmDialog(id='confirm-restart-dialog', message='Are you sure you want to restart the application? This will apply all saved changes.'),
    header, sidebar, content, graph_modal, alarm_modal, 
    WebSocket(url=f"ws://{SERVER_IP}:{WEBSOCKET_PORT}", id="ws"),
    dcc.Store(id='live-update-trigger'),
    dcc.Interval(id="system_info_interval", interval=5000, n_intervals=0),
    dcc.Interval(id="file_update_interval", interval=15000, n_intervals=0),
    dcc.Interval(id="job_update_interval", interval=60000, n_intervals=0)
])

# =========================
# CALLBACKS
# =========================

@app.callback(Output("live-update-trigger", "data"), Input("ws", "message"))
def on_websocket_message(message):
    if not message: return no_update
    return message

@app.callback(Output("page-content", "children"), Input("url", "pathname"))
def display_page(pathname):
    if pathname == "/device": return device_info_layout
    elif pathname == "/data-storage": return data_storage_layout
    elif pathname == "/history": return history_layout
    elif pathname == "/debug": return debug_layout
    elif pathname == "/mqtt": return create_mqtt_layout()
    elif pathname == "/ethernet": return ethernet_layout
    elif pathname == "/modbus": return create_modbus_layout()
    elif pathname == "/opcua": return create_opcua_layout() 
    elif pathname == "/": return dashboard_layout
    return dashboard_layout

@app.callback(Output("debug-log-output", "children"), Input("system_info_interval", "n_intervals"))
def update_debug_log(n):
    with log_lock: return "\n".join(log_messages)

@app.callback(Output("debug-log-output", "children", allow_duplicate=True), Input("clear-log-btn", "n_clicks"), prevent_initial_call=True)
def clear_debug_log(n_clicks):
    if not n_clicks: return no_update
    with log_lock:
        log_messages.clear()
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_messages.appendleft(f"[{timestamp}] --- Log Cleared by User ---")
        return "\n".join(log_messages)

@app.callback(Output("ethernet-settings-card-body", "children"), Input("url", "pathname"))
def update_ethernet_settings(pathname):
    if pathname != "/ethernet": return no_update
    settings = get_ethernet_settings()
    return [dbc.ListGroup([dbc.ListGroupItem([html.Strong("IP Address: "), html.Span(settings["ip_address"])]),dbc.ListGroupItem([html.Strong("Subnet Mask: "), html.Span(settings["subnet_mask"])]),dbc.ListGroupItem([html.Strong("Gateway: "), html.Span(settings["gateway"])]),dbc.ListGroupItem([html.Strong("DNS Servers: "), html.Span(settings["dns_servers"])]),], flush=True), html.Hr(), dbc.Alert([html.I(className="fas fa-info-circle me-2"), "These settings are read-only."], color="info")]

@app.callback(Output("data-storage-container", "children"), Input("url", "pathname"))
def update_data_storage_page(pathname):
    if pathname == "/data-storage": return generate_data_storage_layout()
    return no_update

@app.callback(Output('data-storage-controls-container', 'children'), [Input('data-storage-gateway-select', 'value'), Input("live-update-trigger", "data")])
def update_data_storage_panel(gateway_id, trigger_data):
    if not gateway_id: return dbc.Alert("Please select a serial number.", color="primary")
    with job_lock: active_job = active_jobs.get(gateway_id)
    if active_job:
        control_content = [dbc.Alert([html.H5("Recording in Progress", className="alert-heading"), html.Strong(id={'type': 'time-remaining-display', 'gid': gateway_id}, children=format_time_remaining(active_job['end_time']))], color="danger", style={"textAlign": "center"}), dbc.Button([html.I(className="fas fa-stop me-2"), "Stop Recording"], id='stop-rec-btn', color="danger", className="w-100")]
    else:
        control_content = [dbc.Label("Start New Recording:", className="fw-bold"), dbc.Select(id='rec-duration-select', options=[{"label": "24 Hours", "value": 24}, {"label": "3 Days (72 Hours)", "value": 72}, {"label": "7 Days (168 Hours)", "value": 168}], value=24), dbc.Button([html.I(className="fas fa-play me-2"), "Start Recording"], id='start-rec-btn', color="success", className="w-100 mt-2")]
    
    conn = get_db_connection()
    completed_content = [dbc.ListGroupItem("No completed recordings for this device.", className="text-muted")]
    try:
        cursor = conn.execute("SELECT job_id, start_time, end_time FROM recording_jobs WHERE status='completed' AND gateway_id=? ORDER BY start_time DESC", (gateway_id,))
        job_list = []
        for job_id, start, end in cursor.fetchall():
            job_list.append(dbc.ListGroupItem([dbc.Row([dbc.Col([html.Strong("Recording:"), html.Div(f"{start.strftime('%Y-%m-%d %H:%M')} to {end.strftime('%Y-%m-%d %H:%M')}")], width=True), dbc.Col(dbc.ButtonGroup([dbc.Button([html.I(className="fas fa-download me-1"), " Excel"], id={'type': 'download-job-btn', 'job_id': job_id}, color="primary", outline=True, size="sm"), dbc.Button(html.I(className="fas fa-trash-alt"), id={'type': 'delete-job-btn', 'job_id': job_id}, color="danger", outline=True, size="sm")], className="float-end"), width="auto", className="d-flex align-items-center")], align="center")], id={'type': 'job-list-item', 'job_id': job_id}))
        if job_list: completed_content = job_list
    except Exception: pass
    finally: conn.close()
    return dbc.Row([dbc.Col(control_content, md=4, style={'borderRight': '1px solid #eee'}), dbc.Col([dbc.Label("Completed Recordings:", className="fw-bold mb-2"), dbc.ListGroup(completed_content, flush=True, style={"maxHeight": "300px", "overflowY": "auto"})], md=8)])

@app.callback(Output({'type': 'time-remaining-display', 'gid': ALL}, 'children'), Input('job_update_interval', 'n_intervals'), State({'type': 'time-remaining-display', 'gid': ALL}, 'id'), prevent_initial_call=True)
def update_time_remaining(n, all_displays):
    if not all_displays: return []
    with job_lock: active_job_map = copy.deepcopy(active_jobs)
    outputs = []
    for display in all_displays:
        gid = display['gid']
        if gid in active_job_map: outputs.append(format_time_remaining(active_job_map[gid]['end_time']))
        else: outputs.append("Completing...")
    return outputs

@app.callback(Output('live-update-trigger', 'data', allow_duplicate=True), Input('start-rec-btn', 'n_clicks'), [State('rec-duration-select', 'value'), State('data-storage-gateway-select', 'value')], prevent_initial_call=True)
def start_recording_job(n_clicks, duration_hours, gateway_id):
    if not n_clicks or not gateway_id: return no_update
    try:
        duration_hours_int = int(duration_hours)
        start_time = datetime.now()
        end_time = start_time + timedelta(hours=duration_hours_int)
        job_id = str(uuid.uuid4())
        conn = get_db_connection()
        conn.execute("INSERT INTO recording_jobs (job_id, gateway_id, start_time, end_time, duration_hours, status) VALUES (?, ?, ?, ?, ?, ?)", (job_id, gateway_id, start_time, end_time, duration_hours_int, 'active'))
        conn.commit()
        conn.close()
        with job_lock: active_jobs[gateway_id] = {'job_id': job_id, 'end_time': end_time}
        log_to_web(f"✅ Started new recording job {job_id} for {gateway_id}.")
        return {"type": "job_status_update", "gateway_id": gateway_id}
    except Exception as e:
        log_to_web(f"❌ Error starting recording: {e}")
        return no_update

@app.callback(Output('live-update-trigger', 'data', allow_duplicate=True), Input('stop-rec-btn', 'n_clicks'), State('data-storage-gateway-select', 'value'), prevent_initial_call=True)
def stop_recording_job(n_clicks, gateway_id):
    if not n_clicks or not gateway_id: return no_update
    try:
        with job_lock:
            if gateway_id not in active_jobs: return no_update
            job_id = active_jobs[gateway_id]['job_id']
            del active_jobs[gateway_id]
        conn = get_db_connection()
        conn.execute("UPDATE recording_jobs SET status='completed', end_time=? WHERE job_id=?", (datetime.now(), job_id))
        conn.commit()
        conn.close()
        log_to_web(f"✅ Manually stopped recording job {job_id}.")
        return {"type": "job_status_update", "gateway_id": gateway_id}
    except Exception as e:
        log_to_web(f"❌ Error stopping recording: {e}")
        return no_update

@app.callback(Output('download-excel-component', 'data'), Input({'type': 'download-job-btn', 'job_id': ALL}, 'n_clicks'), prevent_initial_call=True)
def download_job_excel(n_clicks):
    if not ctx.triggered_id or not any(n_clicks): return no_update
    job_id = ctx.triggered_id['job_id']
    try:
        conn = get_db_connection()
        job_info = conn.execute("SELECT gateway_id, start_time, end_time FROM recording_jobs WHERE job_id=?", (job_id,)).fetchone()
        if not job_info: return no_update
        gateway_id, start, end = job_info
        df = pd.read_sql_query("SELECT timestamp, channel, metric_name, value, rpm FROM trends WHERE gateway_id = ? AND timestamp BETWEEN ? AND ? ORDER BY timestamp, channel, metric_name", conn, params=(gateway_id, start, end))
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='All_Trend_Data', index=False)
            if not df.empty:
                try: df.pivot_table(index=['timestamp', 'channel'], columns='metric_name', values='value').reset_index().to_excel(writer, sheet_name='Pivoted_Data', index=False)
                except Exception: pass
        output.seek(0)
        return dcc.send_bytes(output.getvalue(), filename=f"recording_{gateway_id}_{start.strftime('%Y%m%d_%H%M')}.xlsx")
    except Exception as e:
        log_to_web(f"❌ Error generating Excel: {e}")
        return no_update
    finally:
        if conn: conn.close()

@app.callback(Output('live-update-trigger', 'data', allow_duplicate=True), Input({'type': 'delete-job-btn', 'job_id': ALL}, 'n_clicks'), prevent_initial_call=True)
def delete_job_data(n_clicks):
    if not ctx.triggered_id or not any(n_clicks): return no_update
    job_id = ctx.triggered_id['job_id']
    try:
        conn = get_db_connection()
        job_info = conn.execute("SELECT gateway_id, start_time, end_time FROM recording_jobs WHERE job_id=?", (job_id,)).fetchone()
        if not job_info: return no_update
        gateway_id, start, end = job_info
        params = (gateway_id, start, end)
        conn.execute("DELETE FROM trends WHERE gateway_id = ? AND timestamp BETWEEN ? AND ?", params)
        conn.execute("DELETE FROM raw_waveforms WHERE gateway_id = ? AND timestamp BETWEEN ? AND ?", params)
        conn.execute("DELETE FROM raw_spectrum WHERE gateway_id = ? AND timestamp BETWEEN ? AND ?", params)
        conn.execute("DELETE FROM recording_jobs WHERE job_id=?", (job_id,))
        conn.commit()
        conn.close()
        log_to_web(f"✅ Deleted job {job_id}.")
        return {"type": "job_status_update", "gateway_id": gateway_id}
    except Exception as e:
        log_to_web(f"❌ Error deleting job: {e}")
        return no_update

@app.callback(Output("device-dashboard-container", "children"), [Input("live-update-trigger", "data"), Input("device-config-store", "data"), Input({"type": "metric-select-dd", "gateway": ALL, "channel": ALL}, "value"), State('store-accordion-state', 'data')])
def update_dynamic_dashboard(trigger_data, device_config_store, metric_dd_values, active_accordion_item):
    with data_lock: storage_copy = copy.deepcopy(data_storage)
    if not storage_copy: return []
    if not device_config_store: device_config_store = {}
    
    metric_map = {}
    dropdown_inputs = ctx.inputs_list[2]
    if dropdown_inputs and metric_dd_values:
        for i, dropdown_input_dict in enumerate(dropdown_inputs):
            gid = dropdown_input_dict['id'].get('gateway')
            ch = dropdown_input_dict['id'].get('channel')
            if gid and ch and i < len(metric_dd_values): metric_map[(gid, ch)] = metric_dd_values[i]

    accordion_items = []
    sorted_gateway_ids = sorted(storage_copy.keys())
    active_list = active_accordion_item if isinstance(active_accordion_item, list) else [active_accordion_item] if isinstance(active_accordion_item, str) else []
    
    with job_lock: active_job_map = copy.deepcopy(active_jobs)
    completed_job_map = {}
    conn = get_db_connection()
    try:
        cursor = conn.execute("SELECT gateway_id FROM recording_jobs WHERE status='completed' GROUP BY gateway_id")
        for row in cursor.fetchall(): completed_job_map[row[0]] = True
    except Exception: pass
    finally: conn.close()

    for gateway_id in sorted_gateway_ids:
        device_data = storage_copy[gateway_id]
        device_config = device_config_store.get(str(gateway_id), {})
        last_seen_ts = device_data.get('last_seen', 'N/A')
        is_fresh = False
        try:
            if last_seen_ts != 'N/A' and (datetime.now() - datetime.strptime(last_seen_ts, '%Y-%m-%d %H:%M:%S')) < timedelta(seconds=3): is_fresh = True
        except ValueError: pass
        
        rec_indicator = dbc.Badge("Rec", color="danger", className="ms-2", style={"animation": "pulse-red 1.5s infinite", "verticalAlign": "middle"}) if gateway_id in active_job_map else dbc.Badge("Data Ready", color="success", className="ms-2", style={"verticalAlign": "middle"}) if gateway_id in completed_job_map else None
        
        item_id = f"item-{gateway_id}"
        is_open = item_id in active_list
        chevron_icon = html.I(className="fas fa-chevron-up" if is_open else "fas fa-chevron-down", style={'transition': 'transform 0.3s'})
        
        core_header = dbc.Row([
            dbc.Col(dbc.Row([dbc.Col(html.Span(f"Serial: {gateway_id}", style={"backgroundColor": "white", "color": "black", "border": "2px solid #007bff", "padding": "5px 10px", "borderRadius": "15px", "fontWeight": "bold", "marginRight": "15px", "boxShadow": "none", "fontSize": "0.9em"}), width="auto"), dbc.Col(html.Span([html.Span(f"Last Update: {last_seen_ts}", className="text-muted small"), rec_indicator]), width="auto")], align="center", className="g-2"), width=True),
            dbc.Col(dbc.Row([dbc.Col(dbc.Button(html.I(className="fas fa-trash-alt"), id={'type': 'delete-btn', 'gateway': gateway_id}, color="danger", outline=True, size="sm"), width="auto"), dbc.Col(html.Div(chevron_icon, style={'color': '#6c757d', 'fontSize': '1.2em', 'marginLeft': '15px', 'cursor': 'pointer'}), width="auto")], align="center", className="g-2"), width="auto", className="ms-auto d-flex align-items-center")
        ], justify="start", align="center", className="w-100")
        
        styled_header = html.Div(core_header, className="accordion-header-fresh" if is_fresh else "", style={'border': '1px solid #B0E0FF', 'borderRadius': '8px', 'padding': '10px', 'backgroundColor': 'white', 'margin': '5px 0', 'boxShadow': '0 0 8px rgba(176, 224, 255, 0.6)', 'width': '100%', 'display': 'flex', 'alignItems': 'center'})
        
        device_boxes_row = []; any_active = False
        for ch in ['1', '2', '3', '4']:
            ch_data = device_data.get("channels", {}).get(ch)
            if ch_data and ch_data.get("timestamp") and ch_data["timestamp"] != "N/A":
                try: 
                    if (datetime.now() - datetime.strptime(ch_data["timestamp"], '%Y-%m-%d %H:%M:%S')) < timedelta(minutes=5): any_active = True
                except ValueError: pass
            device_boxes_row.append(create_channel_box(ch, gateway_id, ch_data, device_config))
        device_boxes_row.append(create_rpm_box(gateway_id, device_data.get("rpm", 0), any_active, device_config))
        
        graph_content = []
        checks = device_config.get("trends", {})
        if is_open:
            if any(checks.get(k, False) for k in ['1','2','3','4','RPM']):
                graph_content.append(html.Hr(className="my-3"))
                graph_content.append(html.H6("Real-time Trends", className="text-muted mb-3"))
                row_graphs = []; colors = {'1':'blue','2':'green','3':'orange','4':'purple','RPM':'cyan'}
                for ch in ['1', '2', '3', '4']:
                    if not checks.get(ch, False): continue
                    selected_metric = metric_map.get((gateway_id, ch), 'Velocity')
                    ch_data = device_data.get("channels", {}).get(ch)
                    if not ch_data: row_graphs.append(dbc.Col(dbc.Alert(f"Waiting for data CH{ch}...", color="light"), width=12)); continue
                    
                    times = list(ch_data.get("trend_times", [])); values = list(ch_data.get("trends", {}).get(selected_metric, []))
                    fig = go.Figure()
                    fig.update_layout(title=f"CH{ch} - {selected_metric}", template="plotly_white", margin=dict(l=40,r=20,t=40,b=30), height=250, yaxis_title=selected_metric)
                    if times and values: fig.add_trace(go.Scatter(x=times[:min(len(times), len(values))], y=values[:min(len(times), len(values))], line=dict(color=colors[ch], shape='spline')))
                    
                    ctrl = dbc.Row([dbc.Col(dcc.Dropdown(id={'type': 'metric-select-dd', 'gateway': gateway_id, 'channel': ch}, options=[{'label': 'Velocity (mm/s)', 'value': 'Velocity'}, {'label': 'Acceleration (g)', 'value': 'Acceleration'}, {'label': 'Crest Factor', 'value': 'Crest Factor'}, {'label': 'Displacement (µm)', 'value': 'Displacement'}, {'label': 'True Peak (g)', 'value': 'True Peak'}, {'label': 'Std Deviation', 'value': 'Std Deviation'}], value=selected_metric, clearable=False, style={'fontSize': '0.9em'}), width=7), dbc.Col(dbc.Button("Clear", id={'type': 'clear-trend-btn', 'gateway': gateway_id, 'channel': ch}, size="sm", color="warning", outline=True), width="auto", className="ms-auto")], justify="between", className="mb-2")
                    row_graphs.append(dbc.Col([dbc.Card(dbc.CardBody([ctrl, dcc.Graph(figure=fig, config={'displayModeBar': False})]))], lg=6, xs=12, className="mb-3"))
                
                if checks.get("RPM", False):
                    rpm_times = list(device_data.get("rpm_trend_times", [])); rpm_vals = list(device_data.get("rpm_trend", []))
                    fig_rpm = go.Figure()
                    fig_rpm.update_layout(title="RPM Trend", template="plotly_white", margin=dict(l=40,r=20,t=40,b=30), height=250, yaxis_title="RPM")
                    if rpm_times: fig_rpm.add_trace(go.Scatter(x=rpm_times[:min(len(rpm_times), len(rpm_vals))], y=rpm_vals[:min(len(rpm_times), len(rpm_vals))], line=dict(color='cyan', shape='spline')))
                    row_graphs.append(dbc.Col([dbc.Card(dbc.CardBody([html.Div(dbc.Button("Clear RPM", id={'type': 'clear-trend-btn', 'gateway': gateway_id, 'channel': 'RPM'}, size="sm", color="warning", outline=True), className="text-end mb-2"), dcc.Graph(figure=fig_rpm, config={'displayModeBar': False})]))], lg=6, xs=12, className="mb-3"))
                graph_content.append(dbc.Row(row_graphs))
            else: graph_content.append(html.Div(html.Small("Select 'Trend' checkbox on channels to view graphs.", className="text-muted text-center mt-2")))
        
        accordion_items.append(dbc.AccordionItem(children=html.Div([dbc.Row(device_boxes_row, className="g-3 my-2"), html.Div(graph_content)]), title=styled_header, item_id=f"item-{gateway_id}"))
    
    return dbc.Accordion(accordion_items, id="device-accordion", active_item=active_accordion_item, always_open=True, flush=True)

@app.callback(Output('store-accordion-state', 'data'),Input('device-accordion', 'active_item'),prevent_initial_call=True)
def save_accordion_state(active_item): return active_item

@app.callback(Output('live-update-trigger', 'data', allow_duplicate=True), Input({'type': 'clear-trend-btn', 'gateway': ALL, 'channel': ALL}, 'n_clicks'), prevent_initial_call=True)
def clear_specific_trend_data(n_clicks):
    if not ctx.triggered_id or not any(n_clicks): return no_update
    gid = ctx.triggered_id['gateway']; ch = ctx.triggered_id['channel']
    with data_lock:
        if gid in data_storage:
            if ch == 'RPM':
                data_storage[gid]["rpm_trend"].clear(); data_storage[gid]["rpm_trend_times"].clear()
            elif ch in data_storage[gid]["channels"]:
                for metric in TRENDED_METRICS: data_storage[gid]["channels"][ch]["trends"][metric].clear()
                data_storage[gid]["channels"][ch]["trend_times"].clear()
    return {"type": "ui_clear_update", "timestamp": time.time()}

@app.callback(Output("device-info-list", "children"), Input("live-update-trigger", "data"))
def update_device_info(trigger_data):
    with data_lock: storage_copy = copy.deepcopy(data_storage)
    if not storage_copy: return dbc.ListGroupItem("No devices connected.")
    items = []
    for gid in sorted(storage_copy.keys()):
        last = storage_copy[gid].get('last_seen', 'N/A')
        is_stale = True
        try: 
            if last != 'N/A' and (datetime.now() - datetime.strptime(last, '%Y-%m-%d %H:%M:%S')) < timedelta(minutes=5): is_stale = False
        except ValueError: pass
        items.append(dbc.ListGroupItem([dbc.Badge("Offline" if is_stale else "Online", color="danger" if is_stale else "success", className="me-2"), f"Serial: {gid} (Last Seen: {last})"], className="d-flex justify-content-between align-items-center"))
    return dbc.ListGroup(items, flush=True)

@app.callback([Output("graph-modal", "is_open"), Output("modal-header", "children"), Output("modal-graph", "figure"), Output("modal-peak-table-container", "children")], [Input({"type": "ch-wf-btn", "gateway": ALL, "channel": ALL}, "n_clicks_timestamp"), Input({"type": "ch-spec-btn", "gateway": ALL, "channel": ALL}, "n_clicks_timestamp"), Input("modal-close-btn", "n_clicks_timestamp")], [State({"type": "ch-wf-btn", "gateway": ALL, "channel": ALL}, "id"), State({"type": "ch-spec-btn", "gateway": ALL, "channel": ALL}, "id")], prevent_initial_call=True)
def handle_graph_modal(wf_ts, spec_ts, close_ts, wf_ids, spec_ids):
    if not ctx.triggered or ctx.triggered[0]['value'] is None: return [no_update]*4
    wf_c = [t or 0 for t in wf_ts]; spec_c = [t or 0 for t in spec_ts]; close_c = close_ts or 0
    max_wf = max(wf_c, default=0); max_spec = max(spec_c, default=0); max_open = max(max_wf, max_spec)
    if close_c > max_open: return [False, no_update, no_update, no_update]
    
    try:
        if max_wf > max_spec: idx = wf_c.index(max_wf); cid = wf_ids[idx]; gtype = 'wf'
        else: idx = spec_c.index(max_spec); cid = spec_ids[idx]; gtype = 'spec'
        gid = cid['gateway']; ch = cid['channel']
        
        with data_lock:
            d_data = data_storage.get(gid, {})
            c_data = d_data.get("channels", {}).get(ch)
            rpm = d_data.get("rpm", 0)
            if not c_data: return [True, "Error", go.Figure().update_layout(title="No data found"), html.Div()]
            
            ts = c_data.get("timestamp", "N/A")
            if gtype == 'wf':
                y = np.array(c_data.get("waveform_data", [])); fs = c_data.get("sampling_freq", 0)
                if y.size == 0: return [True, "No Waveform Data", go.Figure(), html.Div()]
                x = np.linspace(0, y.size/fs, y.size)
                fig = go.Figure(data=go.Scatter(x=x, y=y.tolist(), mode='lines')).update_layout(title=f"Waveform ({ts})", xaxis_title="Time (s)", yaxis_title="g")
                return [True, f"Waveform - {gid} CH{ch}", fig, html.Div()]
            else:
                y = np.array(c_data.get("spectrum_data", [])); bw = c_data.get("bandwidth", 0); lines = c_data.get("lines", 0)
                if y.size == 0: return [True, "No Spectrum Data", go.Figure(), html.Div()]
                x = np.linspace(0, bw, lines + 1)[:y.size]
                fig = go.Figure(data=go.Scatter(x=x, y=y.tolist(), mode='lines')).update_layout(title=f"Spectrum ({ts})", xaxis_title="Hz", yaxis_title="Amp")
                peaks, _ = find_peaks(y, height=PEAK_HEIGHT_THRESHOLD)
                ptable = html.Div()
                if len(peaks)>0:
                    fig.add_trace(go.Scatter(x=x[peaks], y=y[peaks], mode='markers', marker=dict(color='red', size=8, symbol='x'), name='Peaks'))
                    rows = [html.Tr([html.Td(f"{x[p]:.2f}"), html.Td(f"{y[p]:.4f}")]) for p in peaks]
                    ptable = dbc.Table([html.Thead(html.Tr([html.Th("Freq"), html.Th("Amp")])), html.Tbody(rows)], bordered=True, size='sm')
                if rpm > 0: fig.add_vline(x=rpm/60, line_dash="dash", line_color="red", annotation_text="1x RPM")
                return [True, f"Spectrum - {gid} CH{ch}", fig, ptable]
    except Exception: return [True, "Error", go.Figure(), html.Div()]

@app.callback(Output('device-config-store', 'data'), [Input({"type": "ch-trend-check", "gateway": ALL, "channel": ALL}, "value"), Input({"type": "rpm-trend-check", "gateway": ALL}, "value")], [State('device-config-store', 'data')], prevent_initial_call=True)
def save_trend_check_state(ch_v, rpm_v, store):
    tid = ctx.triggered_id
    if not tid: return no_update
    gid = str(tid['gateway']); ch = tid.get('channel', 'RPM'); val = ctx.triggered[0]['value']
    new_store = copy.deepcopy(store) if store else {}
    if gid not in new_store: new_store[gid] = {}
    if "trends" not in new_store[gid]: new_store[gid]["trends"] = {}
    
    if new_store[gid]["trends"].get(ch) != val:
        new_store[gid]["trends"][ch] = val
        save_device_config_db(gid, new_store[gid])
        return new_store
    return no_update

@app.callback(Output('device-config-store', 'data', allow_duplicate=True), Input({'type': 'ch-label-input', 'gateway': ALL, 'channel': ALL}, 'value'), State('device-config-store', 'data'), prevent_initial_call=True)
def save_channel_label(vals, store):
    if not ctx.triggered_id: return no_update
    gid = str(ctx.triggered_id['gateway']); ch = str(ctx.triggered_id['channel']); val = ctx.triggered[0]['value'] or ""
    new_store = copy.deepcopy(store) if store else {}
    if gid not in new_store: new_store[gid] = {}
    if "labels" not in new_store[gid]: new_store[gid]["labels"] = {}
    if new_store[gid]["labels"].get(ch) != val:
        new_store[gid]["labels"][ch] = val
        save_device_config_db(gid, new_store[gid])
        return new_store
    return no_update

@app.callback([Output("alarm-modal", "is_open"), Output("alarm-modal-header", "children"), Output("alarm-modal-body", "children"), Output('store-alarm-config-data', 'data')], Input({'type': 'ch-alarm-btn', 'gateway': ALL, 'channel': ALL}, 'n_clicks'), State('device-config-store', 'data'), prevent_initial_call=True)
def open_alarm_modal(n, store):
    if not ctx.triggered_id or not ctx.triggered[0]['value']: return [no_update]*4
    gid = str(ctx.triggered_id['gateway']); ch = str(ctx.triggered_id['channel'])
    conf = store.get(gid, {}).get("alarms", {}).get(ch, {})
    body = html.Div([
        dbc.Switch(id='alarm-enable-toggle', label="Enable Alarms", value=conf.get("enabled", False), className="mb-3"), html.Hr(),
        dbc.Label("Warning (mm/s)"), dbc.Input(id='alarm-vel-warn-input', type='number', value=conf.get("vel_warn", 0.0), step=0.1),
        dbc.Label("Alert (mm/s)", className="mt-2"), dbc.Input(id='alarm-vel-alert-input', type='number', value=conf.get("vel_alert", 0.0), step=0.1)
    ])
    return True, f"Alarms - {gid} CH{ch}", body, {'gateway_id': gid, 'channel_id': ch}

@app.callback([Output('device-config-store', 'data', allow_duplicate=True), Output("alarm-modal", "is_open", allow_duplicate=True)], [Input("alarm-modal-save-btn", "n_clicks"), Input("alarm-modal-close-btn", "n_clicks")], [State('store-alarm-config-data', 'data'), State('alarm-enable-toggle', 'value'), State('alarm-vel-warn-input', 'value'), State('alarm-vel-alert-input', 'value'), State('device-config-store', 'data')], prevent_initial_call=True)
def handle_alarm_save(save, close, meta, en, warn, alert, store):
    if ctx.triggered_id == "alarm-modal-close-btn": return no_update, False
    if ctx.triggered_id == "alarm-modal-save-btn" and meta:
        gid = str(meta['gateway_id']); ch = str(meta['channel_id'])
        new_store = copy.deepcopy(store) if store else {}
        if gid not in new_store: new_store[gid] = {}
        if "alarms" not in new_store[gid]: new_store[gid]["alarms"] = {}
        new_store[gid]["alarms"][ch] = {"enabled": en, "vel_warn": float(warn or 0), "vel_alert": float(alert or 0)}
        save_device_config_db(gid, new_store[gid])
        return new_store, False
    return no_update, no_update

@app.callback([Output("pi-serial", "children"), Output("pi-datetime", "children"), Output("pi-uptime", "children"), Output("pi-memory", "children"), Output("pi-temp", "children"), Output("pi-disk", "children"), Output("pi-cpuload", "children"), Output("pi-ip", "children")], Input("system_info_interval", "n_intervals"))
def update_system_info(n):
    mem_t, mem_f = get_memory_usage(); disk_t, disk_f = get_disk_usage()
    return get_pi_serial(), datetime.now().strftime("%Y-%m-%d %H:%M:%S"), get_uptime(), f"{mem_t} / {mem_f}", get_cpu_temp(), f"{disk_t} / {disk_f}", get_cpu_load(), get_ip_address()

# --- HISTORY CALLBACKS ---
@app.callback([Output('history-gateway-select', 'options'), Output('history-gateway-select', 'value')], Input('url', 'pathname'))
def pop_hist_gateways(path):
    if path != '/history': return no_update, no_update
    conn = get_db_connection()
    opts = []
    try:
        cur = conn.execute("SELECT gateway_id FROM config ORDER BY gateway_id")
        opts = [{'label': r[0], 'value': r[0]} for r in cur.fetchall()]
    except Exception: pass
    finally: conn.close()
    return opts, (opts[0]['value'] if opts else None)

@app.callback([Output('history-metric-select', 'options'), Output('history-metric-select', 'value')], Input('history-channel-select', 'value'))
def pop_hist_metrics(ch):
    if ch == 'RPM': return [{'label': 'RPM', 'value': 'rpm'}], 'rpm'
    return [{'label': m, 'value': m} for m in TRENDED_METRICS], 'Velocity'

@app.callback(Output('history-graph', 'figure'), Input('load-history-btn', 'n_clicks'), [State('history-date-picker', 'start_date'), State('history-date-picker', 'end_date'), State('history-gateway-select', 'value'), State('history-channel-select', 'value'), State('history-metric-select', 'value')], prevent_initial_call=True)
def load_history(n, start, end, gid, ch, met):
    if not all([start, end, gid, ch, met]): return go.Figure()
    try:
        end_dt = datetime.fromisoformat(end) + timedelta(days=1)
        conn = get_db_connection()
        q = "SELECT timestamp, value, rpm FROM trends WHERE gateway_id=? AND channel=? AND metric_name=? AND timestamp BETWEEN ? AND ? ORDER BY timestamp ASC"
        p = (gid, ch, met, start, end_dt.isoformat())
        if met == 'rpm':
            q = "SELECT timestamp, rpm FROM trends WHERE gateway_id=? AND channel='1' AND timestamp BETWEEN ? AND ? GROUP BY timestamp ORDER BY timestamp ASC"
            p = (gid, start, end_dt.isoformat())
        df = pd.read_sql_query(q, conn, params=p)
        conn.close()
        fig = go.Figure()
        if not df.empty:
            y_val = df['rpm'] if met == 'rpm' else df['value']
            fig.add_trace(go.Scatter(x=df['timestamp'], y=y_val, mode='lines+markers'))
            fig.update_layout(title=f"{gid} CH{ch} {met}", template="plotly_white")
        else: fig.update_layout(title="No Data Found")
        return fig
    except Exception: return go.Figure()

@app.callback(Output('download-history-component', 'data'), Input('download-history-csv-btn', 'n_clicks'), [State('history-date-picker', 'start_date'), State('history-date-picker', 'end_date'), State('history-gateway-select', 'value'), State('history-channel-select', 'value'), State('history-metric-select', 'value')], prevent_initial_call=True)
def download_history(n, start, end, gid, ch, met):
    if not all([start, end, gid, ch, met]): return no_update
    try:
        end_dt = datetime.fromisoformat(end) + timedelta(days=1)
        conn = get_db_connection()
        q = "SELECT timestamp, value, rpm FROM trends WHERE gateway_id=? AND channel=? AND metric_name=? AND timestamp BETWEEN ? AND ? ORDER BY timestamp ASC"
        p = (gid, ch, met, start, end_dt.isoformat())
        if met == 'rpm':
            q = "SELECT timestamp, rpm FROM trends WHERE gateway_id=? AND channel='1' AND timestamp BETWEEN ? AND ? GROUP BY timestamp ORDER BY timestamp ASC"
            p = (gid, start, end_dt.isoformat())
        df = pd.read_sql_query(q, conn, params=p)
        conn.close()
        return dcc.send_data_frame(df.to_csv, filename=f"hist_{gid}_{ch}_{met}.csv", index=False)
    except Exception: return no_update

# --- RESTART / DELETE CONFIRMATIONS ---
@app.callback([Output('confirm-delete-dialog', 'displayed'), Output('confirm-delete-dialog', 'message'), Output('store-gateway-to-delete', 'data')], Input({'type': 'delete-btn', 'gateway': ALL}, 'n_clicks_timestamp'), prevent_initial_call=True)
def confirm_delete_device(ts):
    if not ctx.triggered or ctx.triggered[0]['value'] is None: return False, no_update, no_update
    gid = ctx.triggered_id['gateway']
    return True, f"Remove device {gid}? This deletes config and stops recording.", gid

@app.callback([Output('device-config-store', 'data', allow_duplicate=True), Output('store-accordion-state', 'data', allow_duplicate=True)], Input('confirm-delete-dialog', 'submit_n_clicks'), [State('store-gateway-to-delete', 'data'), State('device-config-store', 'data')], prevent_initial_call=True)
def do_delete_device(n, gid, store):
    if not n or not gid: return no_update, no_update
    conn = get_db_connection()
    try:
        conn.execute("DELETE FROM trends_latest WHERE gateway_id=?", (gid,))
        conn.execute("DELETE FROM config WHERE gateway_id=?", (gid,))
        conn.commit()
    finally: conn.close()
    with data_lock: data_storage.pop(str(gid), None)
    with job_lock:
        if gid in active_jobs:
            jid = active_jobs[gid]['job_id']
            del active_jobs[gid]
            conn = get_db_connection()
            conn.execute("DELETE FROM recording_jobs WHERE job_id=?", (jid,))
            conn.commit(); conn.close()
    new_store = copy.deepcopy(store); new_store.pop(str(gid), None)
    return new_store, None

@app.callback(Output('confirm-restart-dialog', 'displayed'), Input({'type': 'restart-app-btn', 'page': ALL}, 'n_clicks'), prevent_initial_call=True)
def confirm_restart(n): return True if any(n) else False

@app.callback(Output('live-update-trigger', 'data', allow_duplicate=True), Input('confirm-restart-dialog', 'submit_n_clicks'), prevent_initial_call=True)
def do_restart(n):
    if not n: return no_update
    def reboot():
        time.sleep(1)
        subprocess.run(['sudo', '/sbin/reboot'])
    Thread(target=reboot).start()
    return no_update

@app.callback(Output('sidebar', 'style'), Output("page-content", "style"), Output("sidebar-state-store", "data"), Input("sidebar-toggle-btn", "n_clicks"), State("sidebar-state-store", "data"), prevent_initial_call=True)
def toggle_sidebar(n, is_open):
    if is_open: return {"position": "fixed", "top": 0, "left": "-18rem", "bottom": 0, "width": "18rem", "padding": "1rem", "paddingTop": "4rem", "backgroundColor": "#343a40", "transition": "all 0.3s"}, {"marginLeft": "0rem", "padding": "2rem 1rem", "paddingTop": "5rem", "transition": "all 0.3s"}, False
    return {"position": "fixed", "top": 0, "left": 0, "bottom": 0, "width": "18rem", "padding": "1rem", "paddingTop": "4rem", "backgroundColor": "#343a40", "transition": "all 0.3s"}, {"marginLeft": "18rem", "padding": "2rem 1rem", "paddingTop": "5rem", "transition": "all 0.3s"}, True

# --- DATETIME SETTINGS CALLBACKS ---
@app.callback(Output('time-current-display', 'value'), Input('time-update-interval', 'n_intervals'), State('time-zone-dropdown', 'value'))
def update_time_display(n, tz_str):
    try:
        if tz_str: return datetime.now(pytz.timezone(tz_str)).strftime('%Y-%m-%d %H:%M:%S')
    except Exception: pass
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

@app.callback([Output('time-ntp-server-input', 'disabled'), Output('time-zone-dropdown', 'disabled'), Output('time-sync-btn', 'disabled'), Output('time-autodetect-btn', 'disabled')], Input('time-auto-toggle', 'value'))
def toggle_time_inputs(auto): return auto, auto, auto, auto

@app.callback([Output('time-save-status', 'children'), Output('time-save-status', 'color'), Output('time-save-status', 'is_open')], [Input('time-save-btn', 'n_clicks'), Input('time-sync-btn', 'n_clicks')], [State('time-auto-toggle', 'value'), State('time-ntp-server-input', 'value'), State('time-zone-dropdown', 'value')], prevent_initial_call=True)
def handle_time_buttons(save, sync, auto, ntp, tz):
    tid = ctx.triggered_id
    if tid == 'time-save-btn':
        try:
            subprocess.run(['sudo', 'timedatectl', 'set-ntp', str(auto).lower()])
            if not auto and tz:
                subprocess.run(['sudo', 'timedatectl', 'set-timezone', tz])
            return "Settings saved. Restart to apply.", "success", True
        except Exception as e: return f"Error: {e}", "danger", True
    if tid == 'time-sync-btn':
        if auto: return "Disable Auto-set first.", "warning", True
        try:
            subprocess.run(['sudo', 'ntpdate', ntp])
            return "Time synced!", "success", True
        except Exception as e: return f"Sync failed: {e}", "danger", True
    return no_update, no_update, no_update

# --- MQTT SETTINGS CALLBACKS ---
@app.callback(Output("mqtt-save-status", "is_open"), Input("save-mqtt-btn", "n_clicks"), [State("mqtt-protocol-select", "value"), State("mqtt-host-input", "value"), State("mqtt-port-input", "value"), State("mqtt-user-input", "value"), State("mqtt-pass-input", "value"), State("mqtt-topic-input", "value")], prevent_initial_call=True)
def save_mqtt(n, proto, host, port, user, pw, topic):
    if not n: return no_update
    try:
        conf = {"PROTOCOL": proto, "BROKER": host.strip(), "PORT": int(port), "TOPIC": topic.strip(), "USERNAME": user.strip(), "PASSWORD": pw}
        with open(MQTT_CONFIG_FILE, 'w') as f: json.dump(conf, f, indent=4)
        return True
    except Exception: return no_update

@app.callback([Output("mqtt-test-status", "children"), Output("mqtt-test-status", "color"), Output("mqtt-test-status", "is_open")], Input("test-mqtt-btn", "n_clicks"), [State("mqtt-protocol-select", "value"), State("mqtt-host-input", "value"), State("mqtt-port-input", "value"), State("mqtt-user-input", "value"), State("mqtt-pass-input", "value")], prevent_initial_call=True)
def test_mqtt(n, proto, host, port, user, pw):
    if not n: return no_update, no_update, no_update
    res = {"status": "pending"}
    def on_c(c, u, f, rc): res["status"] = "success" if rc==0 else f"Fail code {rc}"; c.disconnect()
    try:
        c = mqtt.Client()
        if user: c.username_pw_set(user, pw)
        c.on_connect = on_c
        if proto == "mqtts://": c.tls_set(tls_version=ssl.PROTOCOL_TLS)
        c.connect(host, int(port), 5)
        c.loop_start(); time.sleep(3); c.loop_stop()
        if res["status"] == "success": return "Connection Successful!", "success", True
        return f"Connection Failed: {res['status']}", "danger", True
    except Exception as e: return f"Error: {e}", "danger", True

@app.callback([Output("mqtt-status-bar", "children"), Output("mqtt-status-bar", "color")], [Input("mqtt-status-interval", "n_intervals"), Input("live-update-trigger", "data")], prevent_initial_call=True)
def update_mqtt_bar(n, data):
    with mqtt_status_lock: s = copy.deepcopy(mqtt_connection_status)
    color = "success" if s["status"] == "connected" else "danger" if s["status"] == "failed" else "warning"
    return s["message"], color

# --- MODBUS CONFIG CALLBACKS ---
@app.callback([Output('modbus-save-status', 'children'), Output('modbus-save-status', 'color'), Output('modbus-save-status', 'is_open'), Output('live-update-trigger', 'data', allow_duplicate=True), Output('modbus-sensor-select', 'value'), Output('modbus-channel-select', 'value'), Output('modbus-value-select', 'value')], [Input('modbus-enable-switch', 'value'), Input('modbus-add-btn', 'n_clicks'), Input({'type': 'modbus-delete-btn', 'index': ALL}, 'n_clicks'), Input({'type': 'modbus-move-up-btn', 'index': ALL}, 'n_clicks'), Input({'type': 'modbus-move-down-btn', 'index': ALL}, 'n_clicks')], [State('modbus-sensor-select', 'value'), State('modbus-channel-select', 'value'), State('modbus-value-select', 'value')], prevent_initial_call=True)
def handle_modbus(en, add, delete, up, down, sens, ch, field):
    tid = ctx.triggered_id
    if not tid: return [no_update]*7
    conf = load_modbus_config(); regs = conf.get("registers", [])
    msg, color, open_alert = "", "success", False
    
    if tid == 'modbus-enable-switch':
        conf['enabled'] = en; msg = "Modbus Enabled" if en else "Modbus Disabled"; open_alert = True
        save_modbus_config(conf)
    elif tid == 'modbus-add-btn':
        if not sens or not field: return "Select sensor/field", "danger", True, no_update, no_update, no_update, no_update
        ch_save = "RPM" if field == "RPM" else ch
        regs.append({"sensor": sens, "channel": ch_save, "field": field, "data_type": "float"})
        conf['registers'] = regs; save_modbus_config(conf); msg = "Register Added"; open_alert = True
        return msg, color, open_alert, {"type": "modbus_config_change"}, None, None, None
    elif isinstance(tid, dict) and 'index' in tid:
        idx = tid['index']
        if 'modbus-delete-btn' in tid['type'] and delete[idx]: regs.pop(idx)
        elif 'modbus-move-up-btn' in tid['type'] and up[idx] and idx > 0: regs[idx], regs[idx-1] = regs[idx-1], regs[idx]
        elif 'modbus-move-down-btn' in tid['type'] and down[idx] and idx < len(regs)-1: regs[idx], regs[idx+1] = regs[idx+1], regs[idx]
        conf['registers'] = regs; save_modbus_config(conf); msg = "Updated"; open_alert = True

    return msg, color, open_alert, {"type": "modbus_config_change"}, no_update, no_update, no_update

@app.callback(Output('modbus-channel-select', 'disabled'), Input('modbus-value-select', 'value'))
def disable_ch(v): return v == 'RPM'

@app.callback(Output('modbus-table-container', 'children'), Input("live-update-trigger", "data"), prevent_initial_call=True)
def update_modbus_tbl(data):
    try:
        if json.loads(data).get("type") not in ["data_update", "modbus_config_change"]: return no_update
    except: return no_update
    conf = load_modbus_config()
    if not conf.get("enabled"): return no_update
    with data_lock: live = copy.deepcopy(data_storage)
    return generate_modbus_table(conf.get("registers", []), live)

# --- OPC UA CALLBACKS ---
@app.callback(
    Output('opc-live-tree-container', 'children'),
    [Input('live-update-trigger', 'data'),
     Input('opc-server-switch', 'value')],
    prevent_initial_call=True
)
def update_opc_tree(trigger, is_enabled):
    # If the switch is off, show a disabled state
    if not is_enabled:
        return dbc.Alert([
            html.I(className="fas fa-pause-circle me-2"),
            "Server is stopped. Enable the server to browse the address space."
        ], color="warning", className="m-3")

    # 1. Access Data safely
    with data_lock:
        data_snapshot = copy.deepcopy(data_storage)

    if not data_snapshot:
        return dbc.Alert([
            html.I(className="fas fa-spinner fa-spin me-2"),
            "Waiting for sensor data to populate address space..."
        ], color="info", className="m-3")

    # 2. Build the Tree using Accordions
    accordion_items = []

    for gateway_id, device_data in data_snapshot.items():
        # --- Build Content for this Device ---
        device_content = []
        
        # A. RPM Node
        rpm_val = device_data.get("rpm", 0.0)
        device_content.append(
            html.Div([
                html.Div([
                    html.I(className="fas fa-tag me-2 text-warning"), 
                    html.Strong("RPM"),
                    dbc.Badge("Variable", color="light", className="ms-2 border text-muted", style={"fontSize": "0.7em"})
                ]),
                html.Div([
                    html.Span(f"{rpm_val:.2f}", className="badge-value me-2"),
                    dbc.Badge(f"ns=2;s=Device_{gateway_id}.RPM", color="secondary", className="font-monospace", style={"fontSize": "0.7em"})
                ])
            ], className="metric-row")
        )

        # B. Channel Objects
        channels = device_data.get("channels", {})
        for ch_key in sorted(channels.keys()):
            metrics = channels[ch_key].get("metrics", {})
            
            # Channel Header
            device_content.append(
                html.Div([
                    html.I(className="fas fa-folder me-2 text-primary"),
                    html.Strong(f"Channel_{ch_key}")
                ], className="bg-light p-2 mt-2 border-top border-bottom")
            )
            
            # Metric Nodes
            for metric_name, val in metrics.items():
                clean_name = metric_name.replace(" ", "_")
                node_id = f"ns=2;s=Device_{gateway_id}.Channel_{ch_key}.{clean_name}"
                
                device_content.append(
                    html.Div([
                        html.Div([
                            html.I(className="fas fa-tag me-2 text-success" if val > 0 else "fas fa-tag me-2 text-muted"), 
                            html.Span(clean_name)
                        ]),
                        html.Div([
                            html.Span(f"{float(val):.4f}", className="badge-value me-2"),
                            dbc.Badge(node_id, color="light", className="text-dark border font-monospace", style={"fontSize": "0.65em"})
                        ])
                    ], className="metric-row")
                )

        # --- Create Accordion Item for Device ---
        item_header = html.Div([
            html.I(className="fas fa-microchip me-2"),
            f"Device_{gateway_id}",
            dbc.Badge("Object", color="primary", className="ms-2", style={"fontSize": "0.7em"})
        ], className="d-flex align-items-center")

        accordion_items.append(
            dbc.AccordionItem(
                children=html.Div(device_content),
                title=item_header,
                item_id=f"opc-node-{gateway_id}"
            )
        )

    return dbc.Accordion(accordion_items, start_collapsed=False, always_open=True, flush=True)

@app.callback(
    [Output("opc-status-text", "children"),
     Output("opc-status-text", "className"),
     Output("opc-status-icon", "className")],
    Input("opc-server-switch", "value")
)
def toggle_opc_status(is_enabled):
    global opcua_enabled_flag
    opcua_enabled_flag = is_enabled
    # Note: In a real asyncua implementation, stopping the server loop 
    # cleanly from a thread requires distinct signal logic. 
    # Here we update the global flag which logical loops should check.
    
    if is_enabled:
        return "Running", "fw-bold text-success", "fas fa-check-circle fa-3x text-success"
    else:
        return "Stopped", "fw-bold text-danger", "fas fa-stop-circle fa-3x text-danger"

# =========================
# MAIN EXECUTION
# =========================
if __name__ == "__main__":
    # Initialize CPU usage monitoring
    psutil.cpu_percent(interval=None)
    
    # 1. Start MQTT Thread (Data Ingestion)
    mqtt_thread = Thread(target=mqtt_thread_func, daemon=True)
    mqtt_thread.start()
    
    # 2. Start WebSocket Thread (Frontend Push Updates)
    ws_server_thread = Thread(target=start_websocket_server_thread, daemon=True)
    ws_server_thread.start()
    
    # 3. Start Job Manager (Recording Logic)
    job_thread = Thread(target=job_management_thread_func, daemon=True)
    job_thread.start()

    # 4. Start Modbus Server Threads
    modbus_server_thread = Thread(target=modbus_server_thread_func, daemon=True)
    modbus_server_thread.start()
    modbus_updater_thread = Thread(target=modbus_updater_thread_func, daemon=True)
    modbus_updater_thread.start()
    
    # 5. Start OPC UA Server Thread
    # We wrap the event loop runner in a thread
    opcua_thread = Thread(target=opcua_thread_entry, daemon=True)
    opcua_thread.start()
    
    log_to_web("🚀 Dashboard running on http://0.0.0.0:8050")
    
    # Run Dash App
    # use_reloader=False is crucial when using threads to prevent duplication
    app.run(host="0.0.0.0", port=8050, debug=False, use_reloader=False)
