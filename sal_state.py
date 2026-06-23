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
from datetime import datetime, timezone
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

# Device name -> location_id, for the same PIR/presence sensors tracked in
# ROOM_STATE. Built in build_room_state, from the same query, so it can
# never go out of sync with what ROOM_STATE was built from. Used by
# update_unit_states (T-SAL2 increment 2) so on_message can find which room
# a firing sensor belongs to without a database round-trip.
DEVICE_LOCATION = {}

# Device name -> last known value, True or False, for the same sensors.
# Needed because the "true always wins" room aggregation rule means a
# sensor going False can only flip its room to False if every other
# sensor of that type in the room is also currently False — checking that
# requires knowing every sibling sensor's current value, not just the one
# that just fired. Built and kept current alongside DEVICE_LOCATION.
DEVICE_CURRENT_VALUE = {}

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
#     'location_name':     str,
#     'presence_current':  bool,
#     'presence_last_true': datetime or None,
#     'pir_current':        bool,
#     'pir_last_true':      datetime or None,
#     'room_silent':        bool,  — True when all sensors in this room are False
#   }
ROOM_STATE = {}

# Unit-level silence flag. True when every room's room_silent is True
# simultaneously. Set and cleared by update_unit_states. Read by
# sal_silence.py to gate the confirmation timer.
unit_silent = False


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


def validate_time_bands():
    """
    Startup integrity check (T-SAL2 increment 2): confirm the five loaded
    TIME_BANDS cover all 24 hours with no gaps and no overlaps.
    Raises RuntimeError if the check fails.
    """
    expected_bands = {'night', 'morning', 'midday', 'afternoon', 'evening'}
    if set(TIME_BANDS.keys()) != expected_bands:
        raise RuntimeError(
            f'TIME_BANDS validation failed: expected bands {expected_bands}, '
            f'got {set(TIME_BANDS.keys())}'
        )

    ordered = sorted(TIME_BANDS.items(), key=lambda item: item[1]['start_time'])
    for i, (band_name, times) in enumerate(ordered):
        next_band_name, next_times = ordered[(i + 1) % len(ordered)]
        if times['end_time'] != next_times['start_time']:
            raise RuntimeError(
                f'TIME_BANDS validation failed: {band_name} ends at '
                f'{times["end_time"]} but {next_band_name} starts at '
                f'{next_times["start_time"]} — gap or overlap detected'
            )
    print('Time bands validated: 24-hour coverage, no gaps, no overlaps')


def current_time_band():
    """
    Return the band_name (from TIME_BANDS) that the current moment falls
    into. validate_time_bands() runs once at startup and stops the SAL if
    the five bands don't cleanly cover 24 hours — so by the time this
    function is ever called, every moment of the day falls inside exactly
    one band, and a None return should not occur in practice.
    """
    now = datetime.now().time()
    for band_name, times in TIME_BANDS.items():
        start = times['start_time']
        end = times['end_time']
        if start <= end:
            if start <= now < end:
                return band_name
        else:
            if now >= start or now < end:
                return band_name
    return None


# ---------------------------------------------------------------------------
# STARTUP LOADING (D20)
# ---------------------------------------------------------------------------

def load_startup_config(unit):
    """
    Load all D20 startup configuration for this unit into the module-level
    variables above. Call once, before connecting to MQTT.
    """
    global PIR_SENSORS, PRESENCE_SENSORS, EVENT_LIBRARY, EVENT_THRESHOLDS, TIME_BANDS

    pir_rows, presence_rows = sal_db.load_sensor_lists(unit)
    PIR_SENSORS = [row['device'] for row in pir_rows]
    PRESENCE_SENSORS = [row['device'] for row in presence_rows]
    print(f'PIR sensors loaded: {PIR_SENSORS}')
    print(f'Presence sensors loaded: {PRESENCE_SENSORS}')

    EVENT_LIBRARY = {row['event_name']: row for row in sal_db.load_event_library()}
    print(f'Event library loaded: {list(EVENT_LIBRARY.keys())}')

    resident_info = sal_db.load_resident_info(unit)
    resident_id = resident_info['resident_id'] if resident_info else None
    cohort_id = resident_info['clinical_cohort_id'] if resident_info else None

    EVENT_THRESHOLDS = resolve_event_thresholds(
        sal_db.load_event_thresholds(), resident_id, cohort_id
    )
    print(f'Event thresholds resolved for {len(EVENT_THRESHOLDS)} (event, location) combinations')

    TIME_BANDS = resolve_time_bands(
        sal_db.load_time_bands(), resident_id, cohort_id
    )
    print(f'Time bands resolved: {list(TIME_BANDS.keys())}')

    validate_time_bands()


# ---------------------------------------------------------------------------
# PER-ROOM STATE MODEL (D20)
# ---------------------------------------------------------------------------

def build_room_state(unit):
    """
    Build the Per-Room State Model into ROOM_STATE, and the supporting
    DEVICE_LOCATION / DEVICE_CURRENT_VALUE maps, from the same query.

    Each room entry now includes room_silent — True when all sensors in
    that room are currently False. unit_silent is True when every room's
    room_silent is True simultaneously. Both are maintained incrementally
    by update_unit_states on every incoming sensor message.
    """
    global ROOM_STATE, DEVICE_LOCATION, DEVICE_CURRENT_VALUE, unit_silent

    room_state = {}
    for loc in sal_db.load_locations():
        room_state[loc['location_id']] = {
            'location_name': loc['location_name'],
            'presence_current': False,
            'presence_last_true': None,
            'pir_current': False,
            'pir_last_true': None,
            'room_silent': True,
        }

    device_location = {}
    device_current_value = {}

    for row in sal_db.load_room_sensor_state(unit):
        location_id = row['location_id']
        device = row['device']
        device_location[device] = location_id
        device_current_value[device] = (row['last_event_value'] == 'true')

        if location_id not in room_state:
            continue
        room = room_state[location_id]
        value = row['last_event_value']
        last_seen = row['last_seen']

        if row['type'] == 'PIR':
            if value == 'true':
                room['pir_current'] = True
                room['room_silent'] = False
                if room['pir_last_true'] is None or last_seen > room['pir_last_true']:
                    room['pir_last_true'] = last_seen

        elif row['type'] == 'Presence':
            if value == 'true':
                room['presence_current'] = True
                room['room_silent'] = False
            elif value == 'false':
                if room['presence_last_true'] is None or last_seen > room['presence_last_true']:
                    room['presence_last_true'] = last_seen

    ROOM_STATE = room_state
    DEVICE_LOCATION = device_location
    DEVICE_CURRENT_VALUE = device_current_value

    # Compute initial unit_silent from the built room states.
    unit_silent = all(room['room_silent'] for room in ROOM_STATE.values())

    print(f'Per-Room State Model built: {ROOM_STATE}')
    print(f'Device-location map built: {DEVICE_LOCATION}')


def update_unit_states(device, value):
    """
    Update ROOM_STATE and unit_silent for a single incoming PIR/presence
    sensor message. Called by on_message for every PIR/presence event.

    Replaces update_room_state (T-SAL2 increment 3 — WHOLE_APARTMENT_SILENT
    redesign). Now also maintains room_silent per room and unit_silent at
    unit level, so sal_silence.py can gate the confirmation timer purely on
    unit_silent without any room-level checks.

    Per-field rules (unchanged from update_room_state):
    - "true always wins": True immediately sets room *_current to True.
      False can only clear *_current if no sibling sensor of the same type
      is still True.
    - pir_last_true: set on every PIR True (discrete motion event).
    - presence_last_true: set on every presence False (marks end of
      presence period).

    room_silent: True when both pir_current and presence_current are False
    for this room. Updated after every sensor message for the affected room.

    unit_silent: True when every room's room_silent is True. Updated after
    every room_silent change.

    Devices not in DEVICE_LOCATION are silently ignored.
    """
    global DEVICE_CURRENT_VALUE, unit_silent

    location_id = DEVICE_LOCATION.get(device)
    if location_id is None or location_id not in ROOM_STATE:
        return

    DEVICE_CURRENT_VALUE[device] = value
    room = ROOM_STATE[location_id]
    now = datetime.now(timezone.utc)

    if device in PIR_SENSORS:
        if value:
            room['pir_current'] = True
            room['pir_last_true'] = now
        else:
            siblings_true = any(
                DEVICE_CURRENT_VALUE.get(d, False)
                for d in PIR_SENSORS
                if DEVICE_LOCATION.get(d) == location_id
            )
            room['pir_current'] = siblings_true

    elif device in PRESENCE_SENSORS:
        if value:
            room['presence_current'] = True
        else:
            room['presence_last_true'] = now
            siblings_true = any(
                DEVICE_CURRENT_VALUE.get(d, False)
                for d in PRESENCE_SENSORS
                if DEVICE_LOCATION.get(d) == location_id
            )
            room['presence_current'] = siblings_true

    # Recompute room_silent and unit_silent.
    room['room_silent'] = (not room['pir_current'] and not room['presence_current'])
    unit_silent = all(r['room_silent'] for r in ROOM_STATE.values())


def unit_appears_empty():
    """
    EXIT confirmation check (D20, formerly all_pir_silent()). Returns True
    if, right now:
      - every room's pir_last_true is at least exit_confirmation_window_sec
        seconds old, or None (never fired); AND
      - every room's presence_current is False.
    """
    band = current_time_band()
    window_sec = EVENT_THRESHOLDS[('EXIT', None)][band]['confirmation_sec']
    now = datetime.now(timezone.utc)

    for room in ROOM_STATE.values():
        if room['presence_current']:
            return False
        if room['pir_last_true'] is not None:
            elapsed = (now - room['pir_last_true']).total_seconds()
            if elapsed < window_sec:
                return False
    return True


# ---------------------------------------------------------------------------
# LEGACY STATE RESTORATION
# ---------------------------------------------------------------------------

def restore_legacy_state(unit):
    """
    Populate state['door_open'], state['pir_last_seen'], and
    state['presence_status'] from the database, for the existing EXIT/ENTRY
    code.
    """
    devices = [ENTRANCE_DOOR] + PIR_SENSORS
    for device, evt_type, evt_val, last_seen in sal_db.load_legacy_sensor_state(unit, devices):
        if device == ENTRANCE_DOOR:
            state['door_open'] = (evt_val == 'false')
        elif device in PIR_SENSORS:
            state['pir_last_seen'][device] = last_seen

    state['presence_status'] = sal_db.load_current_presence_status(unit)
    print(f'Legacy state restored: {state}')
