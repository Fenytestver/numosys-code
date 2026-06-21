"""
sal_config.py — Configuration constants for the Situational Awareness Layer (SAL)
Nomusys elder care monitoring system, CT 106

All environment-specific values live here. To adapt the SAL to a different
unit, broker, or database, only this file needs to change.

This file was split out of sal.py as part of the D20 SAL implementation
(T-SAL2, increment 1). Values are unchanged from the previous single-file
version — only the location has moved.
"""

# ---------------------------------------------------------------------------
# MQTT BROKER
# ---------------------------------------------------------------------------

MQTT_HOST = '192.168.86.52'   # EMQX broker — CT 102
MQTT_PORT = 1883
MQTT_USER = 'sal'
MQTT_PASS = 'pass1234'

# ---------------------------------------------------------------------------
# POSTGRESQL
# ---------------------------------------------------------------------------

PG_HOST = '192.168.86.55'     # PostgreSQL — CT 105
PG_DB   = 'audittrail'
PG_USER = 'numosys'
PG_PASS = 'pass1234'

# ---------------------------------------------------------------------------
# UNIT IDENTIFICATION
# ---------------------------------------------------------------------------

# Apartment identifier, used in published event payloads and all
# unit-scoped database queries.
UNIT = '215'

# Sensor friendly name as published in the zigbee2mqtt/clean topic.
# Must match the name assigned in Zigbee2MQTT exactly.
ENTRANCE_DOOR = '215_Entrance_Door'  # Single string — only one entrance per apartment

# ---------------------------------------------------------------------------
# TIMING
# ---------------------------------------------------------------------------

# How long all PIR/presence sensors must remain silent after the door closes
# before EXIT is confirmed. 30s is appropriate for lab use; increase for production.
SILENCE_WINDOW_SEC = 120

# Heartbeat interval. The SAL publishes a heartbeat to signal it is alive.
# A future watchdog process will alert if the heartbeat stops arriving.
HEARTBEAT_SEC = 30
