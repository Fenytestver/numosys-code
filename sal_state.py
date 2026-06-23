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
# update_room_state (T-SAL2 increment 2) so on_message can find which room
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


def validate_time_bands():
    """
    Startup integrity check (T-SAL2 increment 2): confirm the five loaded
    TIME_BANDS cover all 24 hours with no gaps and no overlaps, by sorting
    bands by start_time and checking each one's end_time matches the next
    one's start_time, wrapping around so the last band's end_time must
    match the first band's start_time.

    Raises RuntimeError if the check fails. Called once at startup, right
    after TIME_BANDS is resolved — a misconfigured time-band set is a
    deployment error, not something the SAL should run with in a degraded
    state (same principle as D15: a partially-working safety system that
    looks fully functional is worse than one that is openly down). This
    also removes any need for current_time_band() to handle a None result
    downstream — if this check passes, every moment of the day falls
    inside exactly one band.

    Note: the three-level fall-back (institution -> cohort -> resident)
    has already been applied by the time TIME_BANDS is populated — this
    function checks the single resolved set of five bands, not each tier
    separately.
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

    Time bands are wall-clock times for the facility (e.g. 'night' =
    22:00-06:00 local), not UTC — datetime.now() with no tzinfo argument
    returns the system's local time, which is correct here since CT 106
    runs in the facility's local timezone (confirmed: Europe/Budapest).

    A band wraps midnight when start_time > end_time (e.g. night
    22:00-06:00) — for those, "now" falls inside the band if it's at or
    after start_time OR before end_time, not between them.
    """
    now = datetime.now().time()
    for band_name, times in TIME_BANDS.items():
        start = times['start_time']
        end = times['end_time']
        if start <= end:
            if start <= now < end:
                return band_name
        else:
            # Wraps midnight
            if now >= start or now < end:
                return band_name
    return None


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
    cohort_id = resident_info['clinical_cohort_id'] if resident_info else None

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

    # Startup integrity check (T-SAL2 increment 2) — raises RuntimeError
    # and stops the SAL if the five bands don't cleanly cover 24 hours.
    validate_time_bands()


# ---------------------------------------------------------------------------
# PER-ROOM STATE MODEL (D20)
# Built fresh at every startup from sensor_state — the restart-anchor
# pattern used throughout the system. See nomusys_design.md §5, Per-Room
# State Model.
# ---------------------------------------------------------------------------

def build_room_state(unit):
    """
    Build the Per-Room State Model into ROOM_STATE, and the supporting
    DEVICE_LOCATION / DEVICE_CURRENT_VALUE maps (T-SAL2 increment 2), from
    the same query — so all three are always built from the same data and
    can never disagree with each other.

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
    global ROOM_STATE, DEVICE_LOCATION, DEVICE_CURRENT_VALUE

    room_state = {}
    for loc in sal_db.load_locations():
        room_state[loc['location_id']] = {
            'location_name': loc['location_name'],
            'presence_current': False,
            'presence_last_true': None,
            'pir_current': False,
            'pir_last_true': None,
        }

    device_location = {}
    device_current_value = {}

    for row in sal_db.load_room_sensor_state(unit):
        location_id = row['location_id']
        device = row['device']
        device_location[device] = location_id
        device_current_value[device] = (row['last_event_value'] == 'true')

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
    DEVICE_LOCATION = device_location
    DEVICE_CURRENT_VALUE = device_current_value
    print(f'Per-Room State Model built: {ROOM_STATE}')
    print(f'Device-location map built: {DEVICE_LOCATION}')


def update_room_state(device, value):
    """
    Update ROOM_STATE for a single incoming PIR/presence sensor message.
    Called by on_message for every PIR/presence event (T-SAL2 increment 2).

    value is True or False (the sensor's new reported state).

    Mirrors the same per-field rules build_room_state uses, applied
    incrementally to one sensor instead of a full rebuild from the
    database:

    - "true always wins": a sensor reporting True immediately sets its
      room's *_current to True. A sensor reporting False can only set its
      room's *_current to False if no sibling sensor of the same type in
      that room is still True (checked via DEVICE_CURRENT_VALUE) — hence
      DEVICE_CURRENT_VALUE must be updated for this device before that
      check is made.
    - pir_last_true is set to now on every PIR True (discrete event).
    - presence_last_true is set to now on every presence False (marks the
      end of a presence period) — not on True, per the same rule
      build_room_state uses.

    Devices not in DEVICE_LOCATION (not part of the per-room model — e.g.
    the entrance door) are silently ignored; callers should only call this
    for PIR/presence devices.
    """
    global DEVICE_CURRENT_VALUE

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
            # Only clear to False if every PIR sensor in this room is
            # also currently False.
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


def unit_appears_empty():
    """
    EXIT confirmation check (D20, formerly all_pir_silent()). Returns True
    if, right now:
      - every room's pir_last_true is at least exit_confirmation_window_sec
        seconds old, or None (never fired); AND
      - every room's presence_current is False.

    exit_confirmation_window_sec is read from EVENT_THRESHOLDS for the
    current time band — the window can differ by time band even though
    all five bands are currently set to 120s. current_time_band() is
    guaranteed non-None by validate_time_bands() at startup.
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


def whole_apartment_silent_check():
    """
    Called from on_message after every PIR or presence sensor False
    transition. Returns True if the whole apartment has been silent
    beyond the WHOLE_APARTMENT_SILENT threshold, False otherwise.

    'Silent' means: every room's pir_last_true AND presence_last_true are
    both either None or older than the configured threshold for the current
    time band. A room with presence_current=True cannot be silent by
    definition — but presence_current is already False by the time this is
    called (the False transition just arrived and update_room_state has
    already applied it).

    The threshold is read from EVENT_THRESHOLDS for the current time band,
    keyed by ('WHOLE_APARTMENT_SILENT', None) — unit-scoped, no location.
    If the key is missing (not yet configured), returns False safely.

    This function is intentionally message-driven rather than loop-driven.
    WHOLE_APARTMENT_SILENT is an immediate-red emergency condition — waiting
    up to 20 minutes for the next loop tick is clinically unacceptable.
    Checking on every False transition costs negligible CPU and fires within
    seconds of the condition being established.
    """
    key = ('WHOLE_APARTMENT_SILENT', None)
    if key not in EVENT_THRESHOLDS:
        return False

    band = current_time_band()
    threshold_sec = EVENT_THRESHOLDS[key][band]['confirmation_sec']
    now = datetime.now(timezone.utc)

    for room in ROOM_STATE.values():
        # Check PIR
        if room['pir_last_true'] is not None:
            elapsed = (now - room['pir_last_true']).total_seconds()
            if elapsed < threshold_sec:
                return False
        # Check presence
        if room['presence_last_true'] is not None:
            elapsed = (now - room['presence_last_true']).total_seconds()
            if elapsed < threshold_sec:
                return False

    return True
