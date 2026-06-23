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
As of T-SAL2 increment 2, the SAL is split across several files:
  sal_config.py       — configuration constants (MQTT, PostgreSQL, unit, timings)
  sal_db.py            — all database access
  sal_state.py         — in-memory state: sensor lists, loaded event library /
                          thresholds / time bands, the Per-Room State Model,
                          time-band lookup, and legacy door/presence state
  sal_exit_arrival.py  — EXIT and ARRIVAL recognition (D20): the confirmation
                          window, unit_appears_empty() check, the compound EXIT
                          evaluation, and ARRIVAL recognition via
                          write_presence_status
  sal.py               — this file: startup sequence, MQTT wiring, heartbeat,
                          shutdown. on_message routes door messages to
                          sal_exit_arrival's window functions and PIR/presence
                          messages to sal_state.update_room_state plus
                          sal_exit_arrival.write_presence_status.

This file recognises two event types, both via sal_exit_arrival.py:
  EXIT     — the resident has left the apartment
  ARRIVAL  — the resident (or anyone) has returned, recognised as the specific
             state transition AWAY_INFERRED -> IN_ROOM (D20; replaces the
             previous lab-phase ENTRY, which was a door-sequence event).

DEPENDENCIES
------------
  Runtime: Python 3.11+, paho-mqtt, psycopg2-binary
  MQTT broker : EMQX on CT 102 (192.168.86.52:1883), user 'sal'
  PostgreSQL  : CT 105 (192.168.86.55), database 'audittrail', user 'numosys'

DEPLOYMENT
----------
  Deployed as a systemd service: sal.service
  Script location: /opt/sal/sal.py (plus sal_config.py, sal_db.py, sal_state.py,
  sal_exit_arrival.py in the same directory)
  Start/stop: systemctl start|stop sal
  Logs: journalctl -u sal -f

EXIT EVENT LOGIC (D20 — see sal_exit_arrival.py)
-------------------------------------------------
  Trigger : entrance door opens, then closes
  Confirm : sal_state.unit_appears_empty() holds for the full confirmation
            window — every room's PIR has been silent long enough AND every
            room's presence_current is False (Per-Room State Model, both PIR
            and presence sensors, not PIR alone)
  Publish : nomusys/situational/{UNIT}  payload: {event, unit, ts}
  Cancel  : the door reopening cancels the window
  On confirm : sets resident_presence.status = 'AWAY_INFERRED', then runs the
               Compound EXIT Evaluation (mobility classification + time band)

ARRIVAL EVENT LOGIC (D20 — see sal_exit_arrival.py)
------------------------------------------------------
  Trigger : none of its own — recognised inside write_presence_status as the
            specific transition resident_presence.status: AWAY_INFERRED -> IN_ROOM
  Caused by : any PIR or presence sensor reporting True, at any time — no
              window, no confirmation sensor set, no door sequence required
  Publish : nomusys/situational/{UNIT}  payload: {event, unit, ts}
  On confirm : auto-clears RETURN_OVERDUE and the compound EXIT alerts

PRESENCE STATUS RULE
--------------------
  Any PIR or presence sensor activity immediately sets
  resident_presence.status = 'IN_ROOM', via sal_exit_arrival.write_presence_status
  — which is also where the AWAY_INFERRED -> IN_ROOM transition (ARRIVAL) is
  recognised. EXIT confirmation overrides this to AWAY_INFERRED separately,
  through the same function, once the confirmation window passes.

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
import sal_exit_arrival
import sal_door
import sal_loop
from sal_config import (
    MQTT_HOST, MQTT_PORT, MQTT_USER, MQTT_PASS,
    ENTRANCE_DOOR, UNIT, HEARTBEAT_SEC,
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
# EXIT AND ARRIVAL EVENT LOGIC
# Moved to sal_exit_arrival.py as part of T-SAL2 increment 2 (D20).
# start_exit_window, cancel_exit_window, confirm_exit, and the compound
# EXIT evaluation now live there. ARRIVAL (replacing the old ENTRY) is
# recognised by sal_exit_arrival.write_presence_status — every write to
# resident_presence.status in this file goes through that function so
# the AWAY_INFERRED -> IN_ROOM transition is only ever checked in one
# place.
# ---------------------------------------------------------------------------


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
    (contact for door sensors, occupancy/presence for PIR/presence sensors).

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
                print('Entrance door closed — starting confirmation window')
                sal_exit_arrival.start_exit_window(client)
                sal_door.on_door_closed()

            elif contact == False:
                # Door has opened (contact=False = magnet separated = open).
                # Cancel any in-progress confirmation window.
                state['door_open'] = True
                print('Entrance door opened')
                sal_exit_arrival.cancel_exit_window('door reopened')
                sal_door.on_door_opened()

        elif device in sal_state.PIR_SENSORS or device in sal_state.PRESENCE_SENSORS:
            print(f'Sensor message: device={device}, data={data}')
            # D20 (T-SAL2 increment 2): both PIR and presence sensors now feed
            # the Per-Room State Model, not PIR alone — unit_appears_empty()
            # checks presence_current across all rooms, so presence messages
            # must update ROOM_STATE just as PIR messages do.
            value = data.get('occupancy')
            if value is None:
                value = data.get('presence')
            if value is None:
                return  # Message carries neither field (e.g. battery update only)

            sal_state.update_room_state(device, value)

            if value is True:
                # Motion/presence detected. Update last-seen timestamp (legacy
                # field, still used by restore_legacy_state on restart) and set
                # IN_ROOM via write_presence_status — which itself recognises
                # ARRIVAL if this is the AWAY_INFERRED -> IN_ROOM transition.
                # No window check here: ARRIVAL no longer depends on whether an
                # exit confirmation window happens to be active (D20).
                state['pir_last_seen'][device] = datetime.now(timezone.utc)
                state['presence_status'] = sal_exit_arrival.write_presence_status(client, 'IN_ROOM')

            else:
                # Sensor went False. Check for WHOLE_APARTMENT_SILENT — an
                # immediate-red emergency that cannot wait for the 20-minute
                # loop. Evaluated here on every False transition, costs
                # negligible CPU. Only fires when monitoring is active.
                if sal_db.load_monitoring_flag(UNIT):
                    if sal_state.whole_apartment_silent_check():
                        resident_info = sal_db.load_resident_info(UNIT)
                        if resident_info is None:
                            sal_db.insert_technical_alert(
                                UNIT, 'resident_data_missing', 'orange'
                            )
                        else:
                            sal_db.insert_clinical_alert(
                                UNIT,
                                resident_info['resident_uuid'],
                                'whole_apartment_silent_red',
                                'red'
                            )

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
    sal_exit_arrival.cancel_exit_window('SAL shutting down')
    sal_door.on_door_closed()
    sal_door.cancel_door_not_opened_timer('SAL shutting down')
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

# Restore door_open, pir_last_seen, and presence_status from the database
# (sal_state.py). This is the same restart-anchor pattern used throughout
# the system — needed because start_exit_window/write_presence_status in
# sal_exit_arrival.py both read this state.
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

# Start the 20-minute evaluation loop in a daemon thread.
loop_thread = threading.Thread(target=sal_loop.loop_forever, daemon=True)
loop_thread.start()

print('SAL running')
client.loop_forever()   # Hands control to paho; blocks until disconnect or signal
