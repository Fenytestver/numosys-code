"""
sal.py — Situational Awareness Layer (SAL)
Nomusys elder care monitoring system, CT 106

PURPOSE
-------
The SAL bridges raw sensor events and clinical reasoning. It subscribes to the
enriched Zigbee sensor stream published by Node-RED (topic: zigbee2mqtt/clean),
recognises meaningful behavioural events, and publishes named situational events
that the Bayesian engine consumes.

This version recognises two event types:
  EXIT  — the resident has left the apartment
  ENTRY — someone has entered the apartment

DEPENDENCIES
------------
  Runtime: Python 3.11+, paho-mqtt, psycopg2-binary
  MQTT broker : EMQX on CT 102 (192.168.86.52:1883), user 'sal'
  PostgreSQL  : CT 105 (192.168.86.55), database 'audittrail', user 'numosys'
                Tables used: sensor_state (read — sensor watch list + state restore),
                             process_log (write — lifecycle events),
                             residents (read — unit to resident lookup),
                             resident_presence (read/write — presence status),
                             presence_log (write — presence status audit trail),
                             units (read — unit name to id lookup)

DEPLOYMENT
----------
  Deployed as a systemd service: sal.service
  Script location: /opt/sal.py
  Start/stop: systemctl start|stop sal
  Logs: journalctl -u sal -f

EXIT EVENT LOGIC
----------------
  Trigger : entrance door opens, then closes
  Confirm : all PIR/presence sensors silent for SILENCE_WINDOW_SEC after door closes
  Publish : nomusys/situational/{UNIT}  payload: {event, unit, ts}
  Cancel  : any PIR/presence motion during the silence window cancels the countdown
  On confirm : sets resident_presence.status = 'AWAY_INFERRED'

ENTRY EVENT LOGIC
-----------------
  Trigger : entrance door opens, then closes
  Confirm : any PIR/presence sensor fires while the observation window is active
  Publish : nomusys/situational/{UNIT}  payload: {event, unit, ts}
  Cancel  : silence holds for SILENCE_WINDOW_SEC — EXIT is confirmed instead
  On confirm : sets resident_presence.status = 'IN_ROOM'

NOTE: A single observation window (exit_window_active) serves both EXIT and ENTRY.
  EXIT requires silence throughout the window.
  ENTRY requires any PIR/presence activity within the window.
  No separate ENTRY timer or flag is needed — the two events are mutually
  exclusive outcomes of the same window.

PRESENCE STATUS RULE
--------------------
  Any PIR or presence sensor activity immediately sets resident_presence.status = 'IN_ROOM'
  regardless of whether an observation window is active. This ensures that motion
  detected at any point — including outside a window — is reflected in the resident
  record. EXIT confirmed then overrides this to AWAY_INFERRED when silence is confirmed.

CONTACT SENSOR CONVENTION
--------------------------
  contact = True  → door is CLOSED (magnet connected)
  contact = False → door is OPEN   (magnet separated)
  This is the standard Zigbee contact sensor convention and applies to all
  door sensors in the system regardless of manufacturer.

PROCESS LIFECYCLE LOGGING
--------------------------
  On every startup the SAL checks whether its last recorded event was a clean
  shutdown. If yes, it logs 'startup_clean'. If no (crashed or first run), it
  logs 'startup_after_crash'. On SIGTERM or Ctrl+C it logs 'shutdown_clean'.
  These records are written to the process_log table in PostgreSQL and are used
  by operations staff and future monitoring tools to detect unplanned restarts.

DUPLICATE MESSAGE HANDLING
---------------------------
  Some Zigbee sensor models (e.g. IKEA PARASOLL) publish each event twice at
  the firmware level. This is mitigated at the Zigbee2MQTT layer via the global
  debounce setting (device_options.debounce: 0.1 in configuration.yaml).
  The SAL also ignores retained messages replayed by the broker on connect,
  and ignores messages timestamped before the SAL's own startup time.
"""

import json
import signal
import sys
import threading
import time
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
import psycopg2


# ---------------------------------------------------------------------------
# CONFIGURATION
# All environment-specific values live here. To adapt this service to a
# different unit, broker, or database, only this section needs to change.
#
# LAB-PHASE NOTE: In production, ENTRANCE_DOOR and PIR_SENSORS will be
# populated at startup from the database (sensor_assignments table) rather
# than hardcoded here. See design §5 — Confirmation Sensor Configuration.
# This transition depends on T-M1b landing the full v2.0 schema.

MQTT_HOST = '192.168.86.52'   # EMQX broker — CT 102
MQTT_PORT = 1883
MQTT_USER = 'sal'
MQTT_PASS = 'pass1234'

PG_HOST = '192.168.86.55'     # PostgreSQL — CT 105
PG_DB   = 'audittrail'
PG_USER = 'numosys'
PG_PASS = 'pass1234'

# Sensor friendly names as published in the zigbee2mqtt/clean topic.
# These must match the names assigned in Zigbee2MQTT exactly.
ENTRANCE_DOOR = '215_Entrance_Door'  # Single string — only one entrance per apartment

# PIR_SENSORS is populated dynamically at startup by load_pir_sensors(), which
# queries sensor_state for all PIR and Presence sensors assigned to this unit.
# It is not hardcoded here. To pick up a newly added sensor, restart the service:
#   systemctl restart sal
# The list does not refresh while the SAL is running.
#
# PRODUCTION NOTE: In production, live reload is triggered via MQTT
# (nomusys/config/reload/[device]) without a service restart. This depends on
# T-M1b landing the full v2.0 schema (sensor_assignments table). For the lab
# phase and pilot, a manual restart on sensor addition is acceptable.
PIR_SENSORS = []  # Populated at startup by load_pir_sensors()

UNIT = '215'   # Apartment identifier, used in published event payloads

# How long all PIR/presence sensors must remain silent after the door closes
# before EXIT is confirmed. 30s is appropriate for lab use; increase for production.
SILENCE_WINDOW_SEC = 30

# Heartbeat interval. The SAL publishes a heartbeat to signal it is alive.
# A future watchdog process will alert if the heartbeat stops arriving.
HEARTBEAT_SEC = 30


# ---------------------------------------------------------------------------
# STARTUP TIME
# Recorded once at import time. Used to discard messages that were timestamped
# before this process started — catches stale messages that slipped through
# the retained-message filter (msg.retain) due to broker or timing edge cases.
# ---------------------------------------------------------------------------

STARTUP_TIME = datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# IN-MEMORY STATE
# The SAL maintains a small amount of live state between MQTT messages.
# This is not persisted — it is rebuilt from sensor_state on every startup.
# ---------------------------------------------------------------------------

state = {
    'door_open': False,            # True if the entrance door is currently open
    'exit_window_active': False,   # True if we are in the observation window after a door
                                   # close. Used to gate both EXIT confirmation (silence)
                                   # and ENTRY confirmation (PIR fires). A single flag
                                   # serves both — the two windows are always started and
                                   # cancelled together.
    'exit_window_timer': None,     # Reference to the active threading.Timer, if any
    'pir_last_seen': {},           # {device_name: datetime} — last motion timestamp per PIR
    'presence_status': None,       # Last value written to residents.presence_status.
                                   # Populated at startup from the database.
                                   # Writes are skipped when the new value matches this.
}


# ---------------------------------------------------------------------------
# DATABASE HELPERS
# A fresh connection is opened for each write operation. This is intentional:
# the SAL writes infrequently and a persistent connection would require
# keepalive handling. psycopg2 connections are not thread-safe, so opening
# per-call is the safest pattern here.
# ---------------------------------------------------------------------------

def get_db():
    """Open and return a new PostgreSQL connection."""
    return psycopg2.connect(
        host=PG_HOST, dbname=PG_DB,
        user=PG_USER, password=PG_PASS
    )


def log_process_event(event_type, notes=None):
    """
    Write a lifecycle event to the process_log table.

    Valid event_type values are defined by the CHECK constraint on the table:
      startup_clean, startup_after_crash, shutdown_clean, heartbeat_missed

    Failures are printed but not raised — a logging failure must never crash
    the SAL itself.
    """
    try:
        db = get_db()
        cur = db.cursor()
        cur.execute(
            'INSERT INTO process_log (process_name, event_type, notes)'
            ' VALUES (%s, %s, %s)',
            ('sal', event_type, notes)
        )
        db.commit()
        db.close()
        print(f'process_log: {event_type}')
    except Exception as e:
        print(f'process_log error: {e}')


def set_presence_status(status):
    """
    Write presence status to resident_presence for this unit, and append a
    row to presence_log for audit continuity.

    Valid values: 'IN_ROOM', 'AWAY_INFERRED'.
    Skips the database write if the status has not changed since the last
    write — avoids constant database traffic from repeated PIR firings when
    the unit is already IN_ROOM.
    Failures are printed but not raised — a database failure must never
    crash the SAL itself.
    """
    if state['presence_status'] == status:
        return  # No change — skip the write
    try:
        db = get_db()
        cur = db.cursor()
        # Get resident_id for this unit
        cur.execute(
            'SELECT r.id FROM residents r'
            ' WHERE r.unit_id = (SELECT id FROM units WHERE unit_name = %s)'
            '   AND r.status = %s',
            (UNIT, 'Active')
        )
        row = cur.fetchone()
        if not row:
            print(f'presence_status: no Active resident found for unit {UNIT}')
            db.close()
            return
        resident_id = row[0]
        previous_status = state['presence_status'] or 'IN_ROOM'
        # Update resident_presence
        cur.execute(
            'UPDATE resident_presence'
            ' SET status = %s, set_by = %s, set_at = NOW(), updated_at = NOW()'
            ' WHERE resident_id = %s',
            (status, 'system:SAL', resident_id)
        )
        # Append to presence_log
        cur.execute(
            'INSERT INTO presence_log (resident_id, status, previous_status, set_by, set_at)'
            ' VALUES (%s, %s, %s, %s, NOW())',
            (resident_id, status, previous_status, 'system:SAL')
        )
        db.commit()
        db.close()
        state['presence_status'] = status
        print(f'presence_status → {status}')
    except Exception as e:
        print(f'presence_status error: {e}')


def load_pir_sensors():
    """
    Build the PIR_SENSORS watch list from sensor_state at startup.

    Queries sensor_state for all PIR and Presence sensors belonging to this
    unit. The result is stored in the global PIR_SENSORS list and held in
    memory for the lifetime of this process.

    SCALING NOTE: The SAL runs as one process per unit. 200 units = 200 SAL
    processes, each responsible for exactly one unit's sensors. This is the
    intended architecture. Each process is lightweight — a small watch list
    and a handful of timestamps. One process per unit also means a crash in
    one unit's SAL does not affect any other unit.

    REFRESH: The list is loaded once at startup. Adding a new sensor to the
    unit requires a service restart to be picked up:
      systemctl restart sal
    In production, MQTT-based live reload (nomusys/config/reload/[device])
    will handle this without a restart — depends on T-M1b.
    """
    global PIR_SENSORS
    try:
        db = get_db()
        cur = db.cursor()
        cur.execute(
            'SELECT device FROM sensor_state'
            ' WHERE unit_id = (SELECT id FROM units WHERE unit_name = %s)'
            '   AND type IN (%s, %s)',
            (UNIT, 'PIR', 'Presence')
        )
        rows = cur.fetchall()
        db.close()
        PIR_SENSORS = [row[0] for row in rows]
        print(f'PIR/Presence sensors loaded: {PIR_SENSORS}')
    except Exception as e:
        print(f'load_pir_sensors error: {e}')
        PIR_SENSORS = []


# ---------------------------------------------------------------------------
# STARTUP TYPE DETECTION

def get_startup_type():
    """
    Determine whether this startup is clean or follows a crash.

    Reads the most recent process_log entry for this process. If it was a
    clean shutdown, this is a clean start. Any other last state — including
    no prior record at all — is treated as startup_after_crash, which is the
    conservative default.
    """
    try:
        db = get_db()
        cur = db.cursor()
        cur.execute(
            'SELECT event_type FROM process_log'
            ' WHERE process_name = %s'
            ' ORDER BY event_at DESC LIMIT 1',
            ('sal',)
        )
        row = cur.fetchone()
        db.close()
        if row and row[0] == 'shutdown_clean':
            return 'startup_clean'
        return 'startup_after_crash'
    except Exception as e:
        print(f'startup check error: {e}')
        return 'startup_after_crash'


# ---------------------------------------------------------------------------
# STATE RESTORATION
# On startup, the SAL reads the last known sensor values from sensor_state
# (written continuously by Node-RED) so it does not start blind. Without
# this, a restart mid-day would cause the SAL to miss events that occurred
# while it was down, and the first door-close after restart would be
# misinterpreted because door_open would be False regardless of reality.
# ---------------------------------------------------------------------------

def restore_state():
    """
    Populate in-memory state from the last known sensor values in sensor_state,
    and load the current presence_status from the residents table.

    Contact sensor convention: last_event_value is stored as the string 'true'
    or 'false'. contact='false' means the door is OPEN (magnet separated).
    So door_open = (evt_val == 'false').

    Note: last_seen timestamps for PIR sensors are stored as timezone-aware
    datetimes by PostgreSQL. The SAL compares these directly against
    datetime.now(timezone.utc) in all_pir_silent(), so timezone consistency
    is required and is preserved here.
    """
    try:
        db = get_db()
        cur = db.cursor()
        cur.execute(
            'SELECT device, last_event_type, last_event_value, last_seen'
            ' FROM sensor_state'
            ' WHERE device = ANY(%s)',
            ([ENTRANCE_DOOR] + PIR_SENSORS,)
        )
        for row in cur.fetchall():
            device, evt_type, evt_val, last_seen = row
            if device == ENTRANCE_DOOR:
                # contact=false means OPEN; contact=true means CLOSED
                state['door_open'] = (evt_val == 'false')
            elif device in PIR_SENSORS:
                state['pir_last_seen'][device] = last_seen

        # Load current presence status so we don't write redundant updates
        cur.execute(
            'SELECT rp.status FROM resident_presence rp'
            ' JOIN residents r ON r.id = rp.resident_id'
            ' WHERE r.unit_id = (SELECT id FROM units WHERE unit_name = %s)'
            '   AND r.status = %s',
            (UNIT, 'Active')
        )
        row = cur.fetchone()
        if row:
            state['presence_status'] = row[0]

        db.close()
        print(f'State restored: {state}')
    except Exception as e:
        print(f'restore_state error: {e}')


# ---------------------------------------------------------------------------
# EXIT EVENT LOGIC
# The EXIT sequence is:
#   1. Entrance door opens       → door_open = True
#   2. Entrance door closes      → start silence window
#   3. No PIR motion for SILENCE_WINDOW_SEC → EXIT confirmed, publish to MQTT
#   Cancellation: any PIR motion during step 3 cancels the countdown.
# ---------------------------------------------------------------------------

def all_pir_silent():
    """
    Return True if every monitored PIR sensor has been silent for at least
    SILENCE_WINDOW_SEC seconds.

    A sensor with no recorded last_seen (None) is treated as silent — it has
    never fired since the SAL started, which is consistent with absence.
    """
    now = datetime.now(timezone.utc)
    for device, last_seen in state['pir_last_seen'].items():
        if last_seen is not None:
            elapsed = (now - last_seen).total_seconds()
            if elapsed < SILENCE_WINDOW_SEC:
                return False
    return True


def confirm_exit(client):
    """
    Called when the silence window timer expires.

    Re-checks PIR silence at the moment of expiry (a motion event could have
    arrived between the timer firing and this function executing). Only
    publishes EXIT if all sensors are still silent.
    """
    state['exit_window_active'] = False
    state['exit_window_timer'] = None
    if all_pir_silent():
        print('EXIT confirmed — publishing')
        set_presence_status('AWAY_INFERRED')
        payload = json.dumps({
            'event': 'EXIT',
            'unit': UNIT,
            'ts': datetime.now(timezone.utc).isoformat()
        })
        client.publish(f'nomusys/situational/{UNIT}', payload)
    else:
        print('EXIT window expired — PIR active, not confirmed')


def start_exit_window(client):
    """
    Start the silence window countdown after the entrance door closes.

    Guard against double-starting: if a window is already active (e.g. the
    door was closed, reopened, and closed again quickly), the existing window
    continues and a new one is not started.
    """
    if state['exit_window_active']:
        return
    print(f'EXIT window started — {SILENCE_WINDOW_SEC}s')
    state['exit_window_active'] = True
    t = threading.Timer(SILENCE_WINDOW_SEC, confirm_exit, args=[client])
    state['exit_window_timer'] = t
    t.start()


def cancel_exit_window(reason='activity detected'):
    """
    Cancel the active silence window countdown.

    Called when PIR motion is detected during the window, or when the entrance
    door opens again. Accepts a reason string for logging clarity.
    Only prints if a window was actually active — avoids spurious log entries
    when called defensively (e.g. on door-open when no window is running).
    Safe to call when no window is active.
    """
    was_active = state['exit_window_active']
    if state['exit_window_timer']:
        state['exit_window_timer'].cancel()
        state['exit_window_timer'] = None
    state['exit_window_active'] = False
    if was_active:
        print(f'EXIT window cancelled — {reason}')


# ---------------------------------------------------------------------------
# ENTRY EVENT LOGIC
# ENTRY is confirmed when any PIR or presence sensor fires while the
# observation window is active (exit_window_active = True). The same window
# flag serves both EXIT and ENTRY — EXIT requires silence throughout the
# window; ENTRY requires any sensor activity within it. No separate ENTRY
# timer or flag is needed.
# ---------------------------------------------------------------------------

def publish_entry(client):
    """
    Confirm ENTRY: cancel the observation window, set presence_status, publish.
    Called immediately when any PIR fires during the observation window.
    """
    cancel_exit_window('ENTRY confirmed')
    print('ENTRY confirmed — publishing')
    set_presence_status('IN_ROOM')
    payload = json.dumps({
        'event': 'ENTRY',
        'unit': UNIT,
        'ts': datetime.now(timezone.utc).isoformat()
    })
    client.publish(f'nomusys/situational/{UNIT}', payload)


# ---------------------------------------------------------------------------
# MQTT CALLBACKS
# ---------------------------------------------------------------------------

def on_connect(client, userdata, flags, reason_code, properties):
    """
    Called by the MQTT library when the broker connection is established.

    Subscribing here (rather than before connect) ensures the subscription
    is automatically re-established if the connection drops and reconnects.
    """
    print(f'MQTT connected: {reason_code}')
    client.subscribe('zigbee2mqtt/clean')


def on_message(client, userdata, msg):
    """
    Called by the MQTT library for every message on zigbee2mqtt/clean.

    The clean topic carries enriched, normalised sensor messages published by
    the Node-RED 'Clean & Enrich' flow. Each message is a JSON object with
    at minimum: device, unit, type, location, and the sensor-specific fields
    (contact for door sensors, occupancy for PIR sensors).

    Messages are filtered in this order:
      1. Retained messages (replayed by broker on connect) — always ignored.
         These represent the last known state, not a new event.
      2. Messages timestamped before SAL startup — ignored as stale.
         Catches edge cases where retain=False but the message is old.
      3. Messages from devices not in the watch list — silently ignored.
         This topic carries all sensors in the system, not just unit 215.

    Contact sensor convention:
      contact=True  → door CLOSED (magnet connected)
      contact=False → door OPEN   (magnet separated)
    """
    try:
        data = json.loads(msg.payload)

        # Filter 1: discard retained messages replayed by the broker on connect
        if msg.retain:
            return

        # Filter 2: discard messages timestamped before this SAL instance started
        ts = data.get('timestamp')
        if ts:
            msg_time = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            if msg_time < STARTUP_TIME:
                return  # Stale message from before SAL started — ignore

        device = data.get('device')

        if device == ENTRANCE_DOOR:
            contact = data.get('contact')
            if contact is None:
                return  # Message carries no contact field (e.g. battery update only)

            if contact == True and state['door_open']:
                # Door has just closed (contact=True = magnet connected = closed).
                # We only act if we knew it was open — this prevents a false trigger
                # on the first message after startup if the door is already closed.
                state['door_open'] = False
                print('Entrance door closed — starting observation window')
                start_exit_window(client)

            elif contact == False:
                # Door has opened (contact=False = magnet separated = open).
                # Cancel any in-progress observation window.
                state['door_open'] = True
                print('Entrance door opened')
                cancel_exit_window('door reopened')

        elif device in PIR_SENSORS:
            occupancy = data.get('occupancy') or data.get('presence')
            if occupancy is True:
                # Motion detected. Update last-seen timestamp and set IN_ROOM.
                # Any motion always sets IN_ROOM regardless of window state.
                state['pir_last_seen'][device] = datetime.now(timezone.utc)
                set_presence_status('IN_ROOM')
                if state['exit_window_active']:
                    # PIR fired during observation window — someone is in the
                    # apartment. Confirm ENTRY and cancel the window.
                    publish_entry(client)

    except Exception as e:
        print(f'on_message error: {e}')


# ---------------------------------------------------------------------------
# HEARTBEAT
# Runs in a background daemon thread. Publishes a timestamped message every
# HEARTBEAT_SEC seconds to nomusys/system/sal/heartbeat. A future watchdog
# process on Node-RED will subscribe to this topic and raise a technical alert
# if the heartbeat stops arriving (indicating the SAL has crashed or stalled).
# ---------------------------------------------------------------------------

def heartbeat_loop(client):
    """Publish a heartbeat message on a fixed interval. Runs forever."""
    while True:
        time.sleep(HEARTBEAT_SEC)
        client.publish(
            'nomusys/system/sal/heartbeat',
            json.dumps({'ts': datetime.now(timezone.utc).isoformat()})
        )


# ---------------------------------------------------------------------------
# SHUTDOWN HANDLER
# Registered for both SIGTERM (systemctl stop) and SIGINT (Ctrl+C).
# Logs a clean shutdown so the next startup is correctly classified.
# ---------------------------------------------------------------------------

def on_shutdown(signum, frame):
    """Handle graceful shutdown. Cancel any active timers before exiting."""
    print('Shutting down cleanly')
    cancel_exit_window()
    log_process_event('shutdown_clean')
    sys.exit(0)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

# Register shutdown handler before doing anything else
signal.signal(signal.SIGTERM, on_shutdown)
signal.signal(signal.SIGINT, on_shutdown)

# Determine startup type, log it, then restore sensor state from the database
startup_type = get_startup_type()
log_process_event(startup_type)
load_pir_sensors()
restore_state()

# Connect to EMQX. Callbacks are registered before connect() so no messages
# are missed between connection and subscription.
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, clean_session=True)
client.username_pw_set(MQTT_USER, MQTT_PASS)
client.on_connect = on_connect
client.on_message = on_message
client.connect(MQTT_HOST, MQTT_PORT)

# Start the heartbeat in a daemon thread. Daemon threads are automatically
# killed when the main process exits, so no explicit cleanup is needed.
hb = threading.Thread(target=heartbeat_loop, args=[client], daemon=True)
hb.start()

print('SAL running')
client.loop_forever()   # Hands control to paho; blocks until disconnect or signal
