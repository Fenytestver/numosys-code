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
        user=PG_USER, password=PG_PASS,
        client_encoding='UTF8'
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
    Return {'resident_id': ..., 'cohort_id': ..., 'mobility_classification': ...}
    for the Active resident of this unit, or None if there is no Active resident.

    Used to resolve the three-level fall-back (institution → cohort →
    resident) for sal_event_thresholds and time_band_definitions, and to
    evaluate the Compound EXIT block (mobility_classification).
    """
    try:
        db = get_db()
        cur = db.cursor()
        cur.execute(
            'SELECT id, cohort_id, mobility_classification FROM residents'
            ' WHERE unit_id = (SELECT id FROM units WHERE unit_name = %s)'
            '   AND status = %s',
            (unit, 'Active')
        )
        row = cur.fetchone()
        db.close()
        if not row:
            return None
        return {
            'resident_id': row[0],
            'cohort_id': row[1],
            'mobility_classification': row[2]
        }
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


# ---------------------------------------------------------------------------
# CLINICAL ALERTS (T-SAL2 increment 2)
# Base + subtype write (alerts + clinical_alerts), per Data model > Alert
# Architecture. NOTE: publishing to nomusys/alerts/# (T11) is a separate,
# not-yet-built task — these functions write to PostgreSQL only.
# ---------------------------------------------------------------------------

def insert_clinical_alert(unit, cohort_id, reason_code, severity):
    """
    Raise a new clinical alert for this unit. Looks up alert_reason_id from
    alert_reason_codes by code, then performs the required atomic
    two-statement write: INSERT into alerts, then INSERT into
    clinical_alerts with the same id (see Data model > alerts >
    clinical_alerts — D17/D18).

    generated_by is always 'situational' here — these are SAL-raised
    alerts, not Bayesian. bayesian_probability is left NULL, per schema
    (NULL for situational alerts).

    Returns the new alert id, or None on failure.
    """
    try:
        db = get_db()
        cur = db.cursor()
        cur.execute(
            'SELECT id FROM alert_reason_codes WHERE code = %s',
            (reason_code,)
        )
        row = cur.fetchone()
        if not row:
            print(f'insert_clinical_alert error: unknown reason_code {reason_code}')
            db.close()
            return None
        alert_reason_id = row[0]

        cur.execute(
            'INSERT INTO alerts (alert_class, alert_reason_id, severity)'
            ' VALUES (%s, %s, %s) RETURNING id',
            ('clinical', alert_reason_id, severity)
        )
        alert_id = cur.fetchone()[0]

        cur.execute(
            'INSERT INTO clinical_alerts'
            ' (id, cohort_id, unit_id, bayesian_probability, generated_by)'
            ' VALUES (%s, %s, (SELECT id FROM units WHERE unit_name = %s), NULL, %s)',
            (alert_id, cohort_id, unit, 'situational')
        )

        db.commit()
        db.close()
        print(f'Clinical alert raised: {reason_code} ({severity}), id={alert_id}')
        return alert_id
    except Exception as e:
        print(f'insert_clinical_alert error: {e}')
        return None


def find_open_alerts(unit, reason_codes):
    """
    Return [{'alert_id': ..., 'reason_code': ...}, ...] for every alert
    currently status='open' for this unit, matching any of the given
    reason_codes. Used by ARRIVAL's auto-clear to find which alerts to
    close (see Event Categories > ARRIVAL — auto-resolves RETURN_OVERDUE
    and open exit-related alerts).

    self_resolved is reachable only from 'open' (D18) — an alert already
    acknowledged or pending has a human already engaged with it, and is
    closed via 'resolved', not auto-clear. So only status='open' rows
    are eligible here.
    """
    try:
        db = get_db()
        cur = db.cursor()
        cur.execute(
            'SELECT a.id, arc.code'
            ' FROM alerts a'
            ' JOIN clinical_alerts ca ON ca.id = a.id'
            ' JOIN alert_reason_codes arc ON arc.id = a.alert_reason_id'
            ' WHERE ca.unit_id = (SELECT id FROM units WHERE unit_name = %s)'
            '   AND a.status = %s'
            '   AND arc.code = ANY(%s)',
            (unit, 'open', reason_codes)
        )
        rows = cur.fetchall()
        db.close()
        return [{'alert_id': r[0], 'reason_code': r[1]} for r in rows]
    except Exception as e:
        print(f'find_open_alerts error: {e}')
        return []


def auto_clear_alert(alert_id):
    """
    Close an open alert via system auto-clear: write the alert_actions
    row (action_type='auto_cleared', staff_id=NULL — no human actor,
    D18) and update alerts.status to 'self_resolved'. Two-statement
    write in one transaction, same atomicity principle as
    insert_clinical_alert.

    resolved_at is set here the same as it would be for a staff-resolved
    alert — it records when the alert stopped being open, regardless of
    which terminal state it reached.

    Called by ARRIVAL's auto-clear for RETURN_OVERDUE and the exit-related
    alerts (see find_open_alerts).
    """
    try:
        db = get_db()
        cur = db.cursor()
        cur.execute(
            'SELECT id FROM alert_action_types WHERE type_name = %s',
            ('auto_cleared',)
        )
        row = cur.fetchone()
        if not row:
            print('auto_clear_alert error: auto_cleared action type not found')
            db.close()
            return False
        action_type_id = row[0]

        cur.execute(
            'UPDATE alerts SET status = %s, status_since = NOW(),'
            ' resolved_at = NOW() WHERE id = %s',
            ('self_resolved', alert_id)
        )

        cur.execute(
            'INSERT INTO alert_actions'
            ' (alert_id, action_type_id, staff_id, alert_status_after)'
            ' VALUES (%s, %s, NULL, %s)',
            (alert_id, action_type_id, 'self_resolved')
        )

        db.commit()
        db.close()
        print(f'Alert auto-cleared: id={alert_id}')
        return True
    except Exception as e:
        print(f'auto_clear_alert error: {e}')
        return False


def insert_technical_alert(unit, reason_code, severity, device=None):
    """
    Raise a new technical alert for this unit. Same shape as
    insert_clinical_alert — looks up alert_reason_id from
    alert_reason_codes by code, then performs the required atomic
    two-statement write: INSERT into alerts, then INSERT into
    technical_alerts with the same id.

    device is optional (technical_alerts.device is nullable as of this
    session — see nomusys_design.md, technical_alerts table note). Used
    for the SAL's own technical alerts, which are not tied to a single
    failing sensor (e.g. resident_data_missing) — every previous
    technical alert came from the threshold layer and always had a
    device; this is the first one raised by the SAL itself.

    Returns the new alert id, or None on failure.
    """
    try:
        db = get_db()
        cur = db.cursor()
        cur.execute(
            'SELECT id FROM alert_reason_codes WHERE code = %s',
            (reason_code,)
        )
        row = cur.fetchone()
        if not row:
            print(f'insert_technical_alert error: unknown reason_code {reason_code}')
            db.close()
            return None
        alert_reason_id = row[0]

        cur.execute(
            'INSERT INTO alerts (alert_class, alert_reason_id, severity)'
            ' VALUES (%s, %s, %s) RETURNING id',
            ('technical', alert_reason_id, severity)
        )
        alert_id = cur.fetchone()[0]

        cur.execute(
            'INSERT INTO technical_alerts (id, device, unit_id)'
            ' VALUES (%s, %s, (SELECT id FROM units WHERE unit_name = %s))',
            (alert_id, device, unit)
        )

        db.commit()
        db.close()
        print(f'Technical alert raised: {reason_code} ({severity}), id={alert_id}')
        return alert_id
    except Exception as e:
        print(f'insert_technical_alert error: {e}')
        return None

