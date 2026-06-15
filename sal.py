"""
sal.py — Situational Awareness Layer (SAL) entrypoint
Nomusys elder care monitoring system, CT 106

PURPOSE
-------
The SAL bridges raw sensor events and clinical reasoning. It subscribes to the
enriched Zigbee sensor stream published by Node-RED (topic: zigbee2mqtt/clean),
recognises meaningful behavioural events, and publishes named situational events
that the Bayesian engine consumes.

STRUCTURE (D20, T-SAL2)
------------------------
As of T-SAL2 increment 1, the SAL is split across several files:
  sal_config.py — configuration constants (MQTT, PostgreSQL, unit, timings)
  sal_db.py      — all database access
  sal_state.py   — in-memory state: sensor lists, loaded event library /
                   thresholds / time bands, the Per-Room State Model, and
                   the legacy EXIT/ENTRY state
  sal.py         — this file: startup sequence, MQTT wiring, EXIT/ENTRY
                   event logic, heartbeat, shutdown

This file currently still recognises two event types:
  EXIT  — the resident has left the apartment
  ENTRY — someone has entered the apartment

This is the lab-phase EXIT/ENTRY logic, left unchanged by increment 1. The
D20 redesign of EXIT and ARRIVAL (replacing ENTRY) is a later increment
(sal_exit_arrival.py).

DEPENDENCIES
------------
  Runtime: Python 3.11+, paho-mqtt, psycopg2-binary
  MQTT broker : EMQX on CT 102 (192.168.86.52:1883), user 'sal'
  PostgreSQL  : CT 105 (192.168.86.55), database 'audittrail', user 'numosys'

DEPLOYMENT
----------
  Deployed as a systemd service: sal.service
  Script location: /opt/sal.py (plus sal_config.py, sal_db.py, sal_state.py
  in the same directory)
  Start/stop: systemctl start|stop sal
  Logs: journalctl -u sal -f

EXIT EVENT LOGIC
----------------
  Trigger : entrance door opens, then closes
  Confirm : all PIR sensors silent for SILENCE_WINDOW_SEC after door closes
  Publish : nomusys/situational/{UNIT}  payload: {event, unit, ts}
  Cancel  : any PIR motion during the silence window cancels the countdown
  On confirm : sets resident_presence.status = 'AWAY_INFERRED'

ENTRY EVENT LOGIC
-----------------
  Trigger : entrance door opens, then closes
  Confirm : any PIR sensor fires while the observation window is active
  Publish : nomusys/situational/{UNIT}  payload: {event, unit, ts}
  Cancel  : silence holds for SILENCE_WINDOW_SEC — EXIT is confirmed instead
  On confirm : sets resident_presence.status = 'IN_ROOM'

NOTE: A single observation window (exit_window_active) serves both EXIT and ENTRY.
  EXIT requires silence throughout the window.
  ENTRY requires any PIR activity within the window.
  No separate ENTRY timer or flag is needed — the two events are mutually
  exclusive outcomes of the same window.

⚠️ As of this increment, EXIT/ENTRY confirmation reads only PIR_SENSORS,
which is now PIR-only (presence sensors have moved to PRESENCE_SENSORS,
see sal_state.py). This narrows EXIT/ENTRY confirmation to PIR sensors only
for this increment. The D20 redesign (unit_appears_empty(), using the
Per-Room State Model and covering both PIR and presence) replaces this
logic in a later increment.

PRESENCE STATUS RULE
--------------------
  Any PIR sensor activity immediately sets resident_presence.status = 'IN_ROOM'
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

import sal_db
import sal_state
from sal_config import (
    MQTT_HOST, MQTT_PORT, MQTT_USER, MQTT_PASS,
    ENTRANCE_DOOR, UNIT, SILENCE_WINDOW_SEC, HEARTBEAT_SEC,
)
from sal_state import state


# ---------------------------------------------------------------------------
# STARTUP TIME
# Recorded once at import time. Used to discard messages that were timestamped
# before this process started — catches stale messages that slipped through
# the retained-message filter (msg.retain) due to broker or timing edge cases.
# ---------------------------------------------------------------------------

STARTUP_TIME = datetime.now(timezone.utc)


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
        state['presence_status'] = sal_db.set_presence_status(UNIT, 'AWAY_INFERRED', state['presence_status'])
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
# ENTRY is confirmed when any PIR sensor fires while the observation window
# is active (exit_window_active = True). The same window flag serves both
# EXIT and ENTRY — EXIT requires silence throughout the window; ENTRY
# requires any sensor activity within it. No separate ENTRY timer or flag is
# needed.
# ---------------------------------------------------------------------------

def publish_entry(client):
    """
    Confirm ENTRY: cancel the observation window, set presence_status, publish.
    Called immediately when any PIR fires during the observation window.
    """
    cancel_exit_window('ENTRY confirmed')
    print('ENTRY confirmed — publishing')
    state['presence_status'] = sal_db.set_presence_status(UNIT, 'IN_ROOM', state['presence_status'])
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

        elif device in sal_state.PIR_SENSORS:
            occupancy = data.get('occupancy') or data.get('presence')
            if occupancy is True:
                # Motion detected. Update last-seen timestamp and set IN_ROOM.
                # Any motion always sets IN_ROOM regardless of window state.
                state['pir_last_seen'][device] = datetime.now(timezone.utc)
                state['presence_status'] = sal_db.set_presence_status(UNIT, 'IN_ROOM', state['presence_status'])
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
    sal_db.log_process_event('shutdown_clean')
    sys.exit(0)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

# Register shutdown handler before doing anything else
signal.signal(signal.SIGTERM, on_shutdown)
signal.signal(signal.SIGINT, on_shutdown)

# Determine startup type and log it
startup_type = sal_db.get_startup_type()
sal_db.log_process_event(startup_type)

# D20 startup loading (T-SAL2, increment 1): sensor watch lists, event
# library, thresholds, time bands — in the order given by the Startup
# Loading table in nomusys_design.md §5.
sal_state.load_startup_config(UNIT)

# Per-Room State Model (D20): build/restore from sensor_state.
sal_state.build_room_state(UNIT)

# Legacy EXIT/ENTRY state restoration (unchanged from previous version,
# now reading from PIR_SENSORS only — see module docstring).
sal_state.restore_legacy_state(UNIT)

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
