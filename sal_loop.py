"""
sal_loop.py — 20-minute evaluation loop for the Situational Awareness Layer (SAL)
Nomusys elder care monitoring system, CT 106

T-SAL2 increment 3. Runs a background evaluation loop on a fixed period
(LOOP_PERIOD_SEC, default 1200s = 20 minutes). On each tick it evaluates
the following events:

  STATIONARY_PRESENCE  — per room: if presence_current is True and
                         presence_last_true is older than the configured
                         threshold, the resident has been motionless in
                         that room for too long. Two-tier: yellow then
                         orange. Checked per room — two rooms can
                         independently hold open stationary-presence alerts.

  RETURN_OVERDUE       — if presence_status is AWAY_INFERRED and the
                         elapsed time since the last IN_ROOM→AWAY_INFERRED
                         transition exceeds the threshold, the resident has
                         not returned. Two-tier: yellow then orange.

  DOOR_NOT_OPENED      — the morning-band countdown is started here at the
                         first tick after the morning time band begins.
                         The actual alert is raised by sal_door.py when its
                         timer fires; this loop only starts the timer.

WHOLE_APARTMENT_SILENT is intentionally NOT evaluated here. It is an
immediate-red emergency condition — waiting up to 20 minutes for the next
loop tick is clinically unacceptable. Instead, it is evaluated in on_message
(sal.py) on every PIR/presence False transition, via
sal_state.whole_apartment_silent_check(). See sal.py and sal_state.py.

GATING
------
Each tick checks two conditions before evaluating anything:
  1. units.monitoring — read fresh from the database (not held in memory).
     Rationale: this flag can be changed at any time by staff (e.g.
     AWAY_PLANNED sets it FALSE). A stale FALSE would cause the loop to
     skip evaluation during a genuine emergency; a stale TRUE would cause
     it to evaluate during a planned absence. Neither is acceptable. The
     loop period is 20 minutes, so one DB read per tick costs nothing.
  2. resident_presence.status — read from sal_state.state['presence_status'],
     which is kept current by sal_exit_arrival.write_presence_status.

THRESHOLD GRANULARITY NOTE — FOR UI DESIGNERS
----------------------------------------------
STATIONARY_PRESENCE, RETURN_OVERDUE, and DOOR_NOT_OPENED thresholds are
evaluated at most once per loop period (LOOP_PERIOD_SEC). A threshold
configured as "4 hours" may fire anywhere between 4 hours and 4 hours
20 minutes after the condition began, depending on where in the loop cycle
the condition was established. The UI for threshold configuration should
either display this granularity visibly, or constrain threshold inputs to
multiples of LOOP_PERIOD_SEC.
"""

import threading
import time
from datetime import datetime, timezone

import sal_db
import sal_door
import sal_state
from sal_config import UNIT
from sal_state import state


LOOP_PERIOD_SEC = 30   # temporary test — revert to 1200 after testing


# ---------------------------------------------------------------------------
# BAND TRANSITION TRACKING
# Used to detect when the morning band starts, so the DOOR_NOT_OPENED
# timer is started exactly once per day.
# ---------------------------------------------------------------------------

_last_band = None


# ---------------------------------------------------------------------------
# STATIONARY_PRESENCE EVALUATION
# Per-room. Two-tier: yellow threshold exceeded -> yellow alert; orange
# threshold exceeded -> orange alert (replaces yellow if not already orange).
# The dedup check in insert_clinical_alert prevents re-raising while an
# alert for the same room is already open.
# ---------------------------------------------------------------------------

def _evaluate_stationary_presence(resident_info):
    """
    Check each room for a stationary presence condition. Called once per
    loop tick, only when monitoring is active and resident is IN_ROOM.
    """
    now = datetime.now(timezone.utc)
    band = sal_state.current_time_band()

    for location_id, room in sal_state.ROOM_STATE.items():
        if not room['presence_current']:
            continue  # Nobody present in this room right now

        if room['presence_last_true'] is None:
            continue  # No prior False — can't calculate elapsed time

        elapsed = (now - room['presence_last_true']).total_seconds()

        key = ('STATIONARY_PRESENCE', location_id)
        if key not in sal_state.EVENT_THRESHOLDS:
            continue  # Not configured for this room

        thresholds = sal_state.EVENT_THRESHOLDS[key][band]
        orange_sec = thresholds.get('orange_sec')
        yellow_sec = thresholds.get('yellow_sec')

        if orange_sec is not None and elapsed >= orange_sec:
            sal_db.insert_clinical_alert(
                UNIT,
                resident_info['resident_uuid'],
                'stationary_presence_orange',
                'orange',
                location_id=location_id
            )
        elif yellow_sec is not None and elapsed >= yellow_sec:
            sal_db.insert_clinical_alert(
                UNIT,
                resident_info['resident_uuid'],
                'stationary_presence_yellow',
                'yellow',
                location_id=location_id
            )


# ---------------------------------------------------------------------------
# RETURN_OVERDUE EVALUATION
# Two-tier: yellow then orange. Checked when presence_status is AWAY_INFERRED.
# The departure time is read from the most recent presence_log row — the
# last IN_ROOM→AWAY_INFERRED transition timestamp.
# ---------------------------------------------------------------------------

def _evaluate_return_overdue(resident_info):
    """
    Check whether the resident has been AWAY_INFERRED beyond the configured
    thresholds. Called once per loop tick, only when monitoring is active
    and resident_presence.status is AWAY_INFERRED.
    """
    away_since = sal_db.load_away_since(UNIT)
    if away_since is None:
        return  # Cannot determine when they left — skip

    elapsed = (datetime.now(timezone.utc) - away_since).total_seconds()
    band = sal_state.current_time_band()

    key = ('RETURN_OVERDUE', None)
    if key not in sal_state.EVENT_THRESHOLDS:
        return

    thresholds = sal_state.EVENT_THRESHOLDS[key][band]
    orange_sec = thresholds.get('orange_sec')
    yellow_sec = thresholds.get('yellow_sec')

    if orange_sec is not None and elapsed >= orange_sec:
        sal_db.insert_clinical_alert(
            UNIT,
            resident_info['resident_uuid'],
            'return_overdue_orange',
            'orange'
        )
    elif yellow_sec is not None and elapsed >= yellow_sec:
        sal_db.insert_clinical_alert(
            UNIT,
            resident_info['resident_uuid'],
            'return_overdue_yellow',
            'yellow'
        )


# ---------------------------------------------------------------------------
# MAIN LOOP
# ---------------------------------------------------------------------------

def _loop_tick():
    """
    One evaluation cycle. Called every LOOP_PERIOD_SEC by loop_forever().
    """
    global _last_band

    # Gate 1: is monitoring active for this unit?
    if not sal_db.load_monitoring_flag(UNIT):
        print('Loop tick: monitoring inactive — skipped')
        return

    # Band transition: detect morning band start for DOOR_NOT_OPENED.
    current_band = sal_state.current_time_band()
    if current_band == 'morning' and _last_band != 'morning':
        sal_door.start_door_not_opened_timer()
    _last_band = current_band

    # Resident info needed for all alert-raising calls.
    resident_info = sal_db.load_resident_info(UNIT)
    if resident_info is None:
        print('Loop tick: no Active resident — skipping event evaluation')
        return

    presence_status = state['presence_status']

    # Gate 2 (per event): STATIONARY_PRESENCE only when IN_ROOM.
    if presence_status == 'IN_ROOM':
        _evaluate_stationary_presence(resident_info)

    # Gate 2 (per event): RETURN_OVERDUE only when AWAY_INFERRED.
    if presence_status == 'AWAY_INFERRED':
        _evaluate_return_overdue(resident_info)


def loop_forever():
    """
    Run the evaluation loop indefinitely. Intended to run in a daemon
    thread started by sal.py at startup.

    Sleeps LOOP_PERIOD_SEC between ticks. The first tick runs immediately
    on startup (after a short settle delay to allow startup loading to
    complete) so the SAL does not have to wait a full period before its
    first evaluation.
    """
    time.sleep(5)   # Short settle — let startup loading finish
    while True:
        try:
            _loop_tick()
        except Exception as e:
            print(f'Loop tick error: {e}')
        time.sleep(LOOP_PERIOD_SEC)
