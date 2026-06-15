"""
sal_state.py — In-memory state for the Situational Awareness Layer (SAL)
Nomusys elder care monitoring system, CT 106

Everything the SAL holds in memory lives here: the sensor watch lists, the
loaded configuration tables (event library, thresholds, time bands), the
Per-Room State Model, and the legacy state used by the existing EXIT/ENTRY
code. Functions in this file call sal_db.py to fetch data, then decide what
it means and store it in the module-level variables below.

This file was split out of sal.py as part of the D20 SAL implementation
(T-SAL2, increment 1).
"""

import sal_db
from sal_config import ENTRANCE_DOOR


# ---------------------------------------------------------------------------
# LEGACY STATE — RELOCATED FROM sal.py, UNCHANGED FIELDS
# Used by the existing EXIT/ENTRY code, which is left untouched in this
# increment.
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
    'presence_status': None,       # Last value written to resident_presence.status.
                                   # Populated at startup from the database.
                                   # Writes are skipped when the new value matches this.
}


# ---------------------------------------------------------------------------
# D20 STARTUP-LOADED CONFIGURATION (T-SAL2, INCREMENT 1)
# Populated once at startup by load_startup_config(). Held in memory for the
# lifetime of the process — no database round-trips during runtime event
# evaluation.
# ---------------------------------------------------------------------------

# Two separate sensor watch lists (D20) — never combined.
# Each is a plain list of device name strings.
PIR_SENSORS = []
PRESENCE_SENSORS = []

# All active rows from sal_event_library, keyed by event_name.
EVENT_LIBRARY = {}

# Resolved threshold values (institution → cohort → resident fall-back
# already applied), keyed by (event_name, location_id). location_id is None
# for events that are not location-specific. Each value is the thresholds
# JSONB dict (five-band shape) for that event.
EVENT_THRESHOLDS = {}

# Resolved time band definitions (fall-back already applied), keyed by
# band_name. Each value is {'start_time': ..., 'end_time': ...}.
TIME_BANDS = {}

# Per-Room State Model (D20). Keyed by location_id. Each value:
#   {
#     'location_name':    str,
#     'presence_current':  bool,
#     'presence_last_true': datetime or None,
#     'pir_current':        bool,
#     'pir_last_true':      datetime or None,
#   }
ROOM_STATE = {}


# ---------------------------------------------------------------------------
# THREE-LEVEL FALL-BACK RESOLUTION (D10)
# Used for sal_event_thresholds and time_band_definitions. Both tables use
# the same priority: a row for this specific resident, else a row for this
# resident's cohort, else the institution-wide row (resident_id and
# cohort_id both NULL).
# ---------------------------------------------------------------------------

def _pick_best_row(rows, resident_id, cohort_id):
    """
    Given a list of candidate rows (all for the same event/location, or the
    same time band), return the one that wins the three-level fall-back —
    or None if no row applies.
    """
    for row in rows:
        if row['resident_id'] == resident_id:
            return row
    for row in rows:
        if row['resident_id'] is None and row['cohort_id'] == cohort_id:
            return row
    for row in rows:
        if row['resident_id'] is None and row['cohort_id'] is None:
            return row
    return None


def resolve_event_thresholds(raw_rows, resident_id, cohort_id):
    """
    Apply the three-level fall-back to the raw sal_event_thresholds rows.

    Returns a dict keyed by (event_name, location_id) -> thresholds dict.
    """
    candidates = {}
    for row in raw_rows:
        key = (row['event_name'], row['location_id'])
        candidates.setdefault(key, []).append(row)

    resolved = {}
    for key, rows in candidates.items():
        chosen = _pick_best_row(rows, resident_id, cohort_id)
        if chosen is not None:
            resolved[key] = chosen['thresholds']
    return resolved


def resolve_time_bands(raw_rows, resident_id, cohort_id):
    """
    Apply the three-level fall-back to the raw time_band_definitions rows.

    Returns a dict keyed by band_name -> {'start_time': ..., 'end_time': ...}.
    """
    candidates = {}
    for row in raw_rows:
        candidates.setdefault(row['band_name'], []).append(row)

    resolved = {}
    for band_name, rows in candidates.items():
        chosen = _pick_best_row(rows, resident_id, cohort_id)
        if chosen is not None:
            resolved[band_name] = {
                'start_time': chosen['start_time'],
                'end_time': chosen['end_time'],
            }
    return resolved


# ---------------------------------------------------------------------------
# STARTUP LOADING (D20)
# Loads, in the order given by the Startup Loading table in
# nomusys_design.md §5: sensor watch lists, event library, thresholds, time
# bands. Sensor availability (the last row of that table) is not part of
# this increment.
# ---------------------------------------------------------------------------

def load_startup_config(unit):
    """
    Load all D20 startup configuration for this unit into the module-level
    variables above. Call once, before connecting to MQTT.
    """
    global PIR_SENSORS, PRESENCE_SENSORS, EVENT_LIBRARY, EVENT_THRESHOLDS, TIME_BANDS

    # Sensor watch lists — two separate queries, two separate lists.
    pir_rows, presence_rows = sal_db.load_sensor_lists(unit)
    PIR_SENSORS = [row['device'] for row in pir_rows]
    PRESENCE_SENSORS = [row['device'] for row in presence_rows]
    print(f'PIR sensors loaded: {PIR_SENSORS}')
    print(f'Presence sensors loaded: {PRESENCE_SENSORS}')

    # Event definitions — all active rows, keyed by event_name.
    EVENT_LIBRARY = {row['event_name']: row for row in sal_db.load_event_library()}
    print(f'Event library loaded: {list(EVENT_LIBRARY.keys())}')

    # Resident/cohort context for the fall-back resolution.
    resident_info = sal_db.load_resident_info(unit)
    resident_id = resident_info['resident_id'] if resident_info else None
    cohort_id = resident_info['cohort_id'] if resident_info else None

    # Threshold values — resolved via three-level fall-back.
    EVENT_THRESHOLDS = resolve_event_thresholds(
        sal_db.load_event_thresholds(), resident_id, cohort_id
    )
    print(f'Event thresholds resolved for {len(EVENT_THRESHOLDS)} (event, location) combinations')

    # Time band definitions — resolved via three-level fall-back.
    TIME_BANDS = resolve_time_bands(
        sal_db.load_time_bands(), resident_id, cohort_id
    )
    print(f'Time bands resolved: {list(TIME_BANDS.keys())}')


# ---------------------------------------------------------------------------
# PER-ROOM STATE MODEL (D20)
# Built fresh at every startup from sensor_state — the restart-anchor
# pattern used throughout the system. See nomusys_design.md §5, Per-Room
# State Model.
# ---------------------------------------------------------------------------

def build_room_state(unit):
    """
    Build the Per-Room State Model into ROOM_STATE.

    Every active location gets a room entry, even if no PIR or presence
    sensor is deployed there yet — so a sensor added later (and picked up on
    the next restart) has somewhere to report into.

    Aggregation rule ("true always wins"): within each sensor group (PIR,
    presence), if any sensor in the room currently reports True, the
    room-level *_current value is True.

    pir_last_true: the most recent last_seen among PIR sensors whose current
    value is 'true' (each PIR True is a discrete motion event).

    presence_last_true: the most recent last_seen among presence sensors
    whose current value is 'false' (a False message marks the end of a
    presence period). A presence sensor currently reporting 'true' has no
    prior False on record, so it contributes nothing to
    presence_last_true — that room's presence_last_true stays None unless
    another presence sensor in the room has a recorded False.
    """
    global ROOM_STATE

    room_state = {}
    for loc in sal_db.load_locations():
        room_state[loc['location_id']] = {
            'location_name': loc['location_name'],
            'presence_current': False,
            'presence_last_true': None,
            'pir_current': False,
            'pir_last_true': None,
        }

    for row in sal_db.load_room_sensor_state(unit):
        location_id = row['location_id']
        if location_id not in room_state:
            # Sensor is assigned to a location not in the active locations
            # list — shouldn't happen, but don't let it crash startup.
            continue
        room = room_state[location_id]
        value = row['last_event_value']
        last_seen = row['last_seen']

        if row['type'] == 'PIR':
            if value == 'true':
                room['pir_current'] = True
                if room['pir_last_true'] is None or last_seen > room['pir_last_true']:
                    room['pir_last_true'] = last_seen
            # value == 'false' carries no timestamp information for
            # pir_last_true — PIR True is a discrete event, not a period.

        elif row['type'] == 'Presence':
            if value == 'true':
                room['presence_current'] = True
                # No prior False on record for this sensor — does not set
                # presence_last_true (see docstring).
            elif value == 'false':
                if room['presence_last_true'] is None or last_seen > room['presence_last_true']:
                    room['presence_last_true'] = last_seen

    ROOM_STATE = room_state
    print(f'Per-Room State Model built: {ROOM_STATE}')


# ---------------------------------------------------------------------------
# LEGACY STATE RESTORATION — RELOCATED FROM sal.py, UNCHANGED LOGIC
# Restores door_open, pir_last_seen, and presence_status for the existing
# EXIT/ENTRY code. pir_last_seen is now populated from PIR_SENSORS only
# (PIR_SENSORS no longer includes presence sensors — see session discussion
# for T-SAL2 increment 1).
# ---------------------------------------------------------------------------

def restore_legacy_state(unit):
    """
    Populate state['door_open'], state['pir_last_seen'], and
    state['presence_status'] from the database, for the existing EXIT/ENTRY
    code.

    Contact sensor convention: last_event_value is stored as the string
    'true' or 'false'. contact='false' means the door is OPEN (magnet
    separated). So door_open = (evt_val == 'false').
    """
    devices = [ENTRANCE_DOOR] + PIR_SENSORS
    for device, evt_type, evt_val, last_seen in sal_db.load_legacy_sensor_state(unit, devices):
        if device == ENTRANCE_DOOR:
            state['door_open'] = (evt_val == 'false')
        elif device in PIR_SENSORS:
            state['pir_last_seen'][device] = last_seen

    state['presence_status'] = sal_db.load_current_presence_status(unit)
    print(f'Legacy state restored: {state}')
