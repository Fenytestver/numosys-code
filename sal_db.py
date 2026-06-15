"""
sal_db.py — Database access for the Situational Awareness Layer (SAL)
Nomusys elder care monitoring system, CT 106

All functions that talk to PostgreSQL live here. Nothing in this file holds
state between calls — every function opens a connection, does its work, and
closes it again. This file does not decide what the data MEANS; it only
fetches it. Turning the fetched rows into the SAL's in-memory state happens
in sal_state.py.

PostgreSQL: CT 105 (192.168.86.55), database 'audittrail', user 'numosys'

This file was split out of sal.py as part of the D20 SAL implementation
(T-SAL2, increment 1).
"""

import psycopg2
from psycopg2.extras import RealDictCursor

from sal_config import PG_HOST, PG_DB, PG_USER, PG_PASS


def get_db():
    """Open and return a new PostgreSQL connection."""
    return psycopg2.connect(
        host=PG_HOST, dbname=PG_DB,
        user=PG_USER, password=PG_PASS
    )


# ---------------------------------------------------------------------------
# RELOCATED FROM sal.py — UNCHANGED BEHAVIOUR
# These existed before this increment and are moved here as-is. They support
# the existing EXIT/ENTRY logic, which is left untouched in this increment.
# ---------------------------------------------------------------------------

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


def set_presence_status(unit, status, current_status):
    """
    Write presence status to resident_presence for this unit, and append a
    row to presence_log for audit continuity.

    Valid values: 'IN_ROOM', 'AWAY_INFERRED'.

    current_status is the caller's in-memory record of the last status
    written (or None if not yet known). If it matches `status`, the write is
    skipped — this avoids constant database traffic from repeated PIR firings
    when the unit is already IN_ROOM.

    Returns the new status if a write happened, or current_status unchanged
    if the write was skipped or failed. The caller is responsible for storing
    this return value back into its own state.

    Failures are printed but not raised — a database failure must never
    crash the SAL itself.
    """
    if current_status == status:
        return current_status  # No change — skip the write
    try:
        db = get_db()
        cur = db.cursor()
        # Get resident_id for this unit
        cur.execute(
            'SELECT r.id FROM residents r'
            ' WHERE r.unit_id = (SELECT id FROM units WHERE unit_name = %s)'
            '   AND r.status = %s',
            (unit, 'Active')
        )
        row = cur.fetchone()
        if not row:
            print(f'presence_status: no Active resident found for unit {unit}')
            db.close()
            return current_status
        resident_id = row[0]
        previous_status = current_status or 'IN_ROOM'
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
        print(f'presence_status → {status}')
        return status
    except Exception as e:
        print(f'presence_status error: {e}')
        return current_status


def load_legacy_sensor_state(unit, devices):
    """
    Read last_event_type, last_event_value, and last_seen for a given list of
    devices belonging to this unit.

    Used by the existing restore_state() to rebuild door_open and
    pir_last_seen on startup. Unchanged behaviour from the previous
    single-file version, beyond taking `devices` as a parameter instead of
    reading a module-level global.
    """
    try:
        db = get_db()
        cur = db.cursor()
        cur.execute(
            'SELECT device, last_event_type, last_event_value, last_seen'
            ' FROM sensor_state'
            ' WHERE device = ANY(%s)',
            (devices,)
        )
        rows = cur.fetchall()
        db.close()
        return rows
    except Exception as e:
        print(f'load_legacy_sensor_state error: {e}')
        return []


def load_current_presence_status(unit):
    """
    Read the current resident_presence.status for the Active resident of
    this unit. Returns None if no Active resident or no presence row exists.
    """
    try:
        db = get_db()
        cur = db.cursor()
        cur.execute(
            'SELECT rp.status FROM resident_presence rp'
            ' JOIN residents r ON r.id = rp.resident_id'
            ' WHERE r.unit_id = (SELECT id FROM units WHERE unit_name = %s)'
            '   AND r.status = %s',
            (unit, 'Active')
        )
        row = cur.fetchone()
        db.close()
        return row[0] if row else None
    except Exception as e:
        print(f'load_current_presence_status error: {e}')
        return None


# ---------------------------------------------------------------------------
# NEW FOR D20 (T-SAL2, INCREMENT 1) — STARTUP LOADING
# Each function below corresponds to one row of the Startup Loading table in
# nomusys_design.md §5 (Situational Awareness Layer > Startup Loading — D20).
# All return plain data (lists of dicts) — no in-memory state is built here.
# ---------------------------------------------------------------------------

def load_resident_info(unit):
    """
    Return {'resident_id': ..., 'cohort_id': ...} for the Active resident of
    this unit, or None if there is no Active resident.

    Used to resolve the three-level fall-back (institution → cohort →
    resident) for sal_event_thresholds and time_band_definitions.
    """
    try:
        db = get_db()
        cur = db.cursor()
        cur.execute(
            'SELECT id, cohort_id FROM residents'
            ' WHERE unit_id = (SELECT id FROM units WHERE unit_name = %s)'
            '   AND status = %s',
            (unit, 'Active')
        )
        row = cur.fetchone()
        db.close()
        if not row:
            return None
        return {'resident_id': row[0], 'cohort_id': row[1]}
    except Exception as e:
        print(f'load_resident_info error: {e}')
        return None


def load_sensor_lists(unit):
    """
    Return (pir_sensors, presence_sensors) — two separate lists.

    Each entry is a dict: {'device': ..., 'location_id': ...}.

    PIR and Presence sensors are queried separately (D20 — they are never
    combined into a single list). location_id is included because the
    Per-Room State Model groups sensors by room.
    """
    try:
        db = get_db()
        cur = db.cursor(cursor_factory=RealDictCursor)
        unit_id_subquery = '(SELECT id FROM units WHERE unit_name = %s)'

        cur.execute(
            'SELECT device, location_id FROM sensor_state'
            f' WHERE unit_id = {unit_id_subquery} AND type = %s',
            (unit, 'PIR')
        )
        pir_sensors = cur.fetchall()

        cur.execute(
            'SELECT device, location_id FROM sensor_state'
            f' WHERE unit_id = {unit_id_subquery} AND type = %s',
            (unit, 'Presence')
        )
        presence_sensors = cur.fetchall()

        db.close()
        return pir_sensors, presence_sensors
    except Exception as e:
        print(f'load_sensor_lists error: {e}')
        return [], []


def load_event_library():
    """
    Return all active rows from sal_event_library as a list of dicts.

    The SAL holds these in memory for the lifetime of the process — no
    database round-trips during runtime event evaluation.
    """
    try:
        db = get_db()
        cur = db.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            'SELECT event_name, category, trigger_description,'
            ' confirmation_rule, output_type, direct_alert_severity, notes'
            ' FROM sal_event_library'
            ' WHERE active = TRUE'
        )
        rows = cur.fetchall()
        db.close()
        return rows
    except Exception as e:
        print(f'load_event_library error: {e}')
        return []


def load_event_thresholds():
    """
    Return all rows from sal_event_thresholds as a list of dicts.

    Includes institution-wide, cohort, and per-resident rows. The three-level
    fall-back resolution (institution → cohort → resident) happens in
    sal_state.py — this function just returns the raw rows.
    """
    try:
        db = get_db()
        cur = db.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            'SELECT event_name, location_id, resident_id, cohort_id,'
            ' thresholds, source'
            ' FROM sal_event_thresholds'
        )
        rows = cur.fetchall()
        db.close()
        return rows
    except Exception as e:
        print(f'load_event_thresholds error: {e}')
        return []


def load_time_bands():
    """
    Return all rows from time_band_definitions as a list of dicts.

    Includes institution-wide, cohort, and per-resident rows. The three-level
    fall-back resolution happens in sal_state.py.
    """
    try:
        db = get_db()
        cur = db.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            'SELECT band_name, start_time, end_time, resident_id, cohort_id,'
            ' source'
            ' FROM time_band_definitions'
        )
        rows = cur.fetchall()
        db.close()
        return rows
    except Exception as e:
        print(f'load_time_bands error: {e}')
        return []


def load_locations():
    """
    Return all active rows from locations as a list of dicts:
    {'location_id': ..., 'location_name': ...}.

    Used to build the Per-Room State Model — every active location gets a
    room entry, even if no PIR or presence sensor is deployed there yet.
    """
    try:
        db = get_db()
        cur = db.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            'SELECT id AS location_id, location_name FROM locations'
            ' WHERE active = TRUE'
        )
        rows = cur.fetchall()
        db.close()
        return rows
    except Exception as e:
        print(f'load_locations error: {e}')
        return []


def load_room_sensor_state(unit):
    """
    Return one row per PIR/Presence sensor in this unit, for building and
    restoring the Per-Room State Model:
    {'device': ..., 'type': ..., 'location_id': ...,
     'last_event_value': ..., 'last_seen': ...}.

    last_event_value is the string 'true' or 'false' as stored by the
    sensor_state upsert. 'true' means: PIR — motion detected;
    Presence — someone present.
    """
    try:
        db = get_db()
        cur = db.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            'SELECT device, type, location_id, last_event_value, last_seen'
            ' FROM sensor_state'
            ' WHERE unit_id = (SELECT id FROM units WHERE unit_name = %s)'
            '   AND type IN (%s, %s)',
            (unit, 'PIR', 'Presence')
        )
        rows = cur.fetchall()
        db.close()
        return rows
    except Exception as e:
        print(f'load_room_sensor_state error: {e}')
        return []
