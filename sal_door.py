"""
sal_door.py — Door-timer events for the Situational Awareness Layer (SAL)
Nomusys elder care monitoring system, CT 106

T-SAL2 increment 3. Handles the two door-state events whose detection is
timer-based and message-driven (not loop-driven):

DOOR_LEFT_OPEN
    Trigger : entrance door opens (contact=False message arrives).
    Mechanism: a single countdown timer starts. If the door is still open
               when the timer expires, a clinical alert is raised.
    Cancel  : door closes (contact=True) before the timer fires.
    Alert   : door_left_open_yellow (single tier — no escalation chain).

DOOR_NOT_OPENED
    Trigger : morning time band starts (called from sal_loop.py at the
              first loop tick after the band begins).
    Mechanism: a single countdown timer starts. If the door has not opened
               before the timer fires, a clinical alert is raised.
    Cancel  : door opens during the countdown — normal morning activity.
    Reset   : fires once per day. After the alert fires or the door opens,
              the timer is not restarted until the next morning band.

Both timers use threading.Timer (same pattern as the EXIT confirmation
window in sal_exit_arrival.py). Both are single-timer-chain events —
only one timer of each type runs at a time, and the single-timer-chain
structure prevents duplicate alerts structurally, without needing the
insert_clinical_alert dedup check. The dedup check in insert_clinical_alert
still applies as a safety net.

Threshold values are read from EVENT_THRESHOLDS at the moment the timer
fires, not when it starts — so a threshold change mid-countdown takes
effect at evaluation time, not start time. This is consistent with the
rest of the SAL.
"""

import threading
from datetime import datetime, timezone
import json

import sal_db
import sal_state
from sal_config import UNIT


# ---------------------------------------------------------------------------
# MODULE-LEVEL TIMER STATE
# One slot per event type. None means no timer is currently running.
# ---------------------------------------------------------------------------

_door_left_open_timer = None
_door_not_opened_timer = None

# Tracks whether the door has opened during the current day's morning band.
# Set to True by on_door_opened(); reset to False by start_door_not_opened_timer()
# at the start of each new morning band.
_door_opened_this_morning = False


# ---------------------------------------------------------------------------
# DOOR_LEFT_OPEN
# ---------------------------------------------------------------------------

def on_door_opened():
    """
    Called by sal.py on_message when the entrance door opens (contact=False).

    Starts the DOOR_LEFT_OPEN countdown timer. If a timer is already running
    (door opened twice without closing — shouldn't happen in practice but
    guarded against), the existing timer continues unchanged.

    Also records that the door has opened this morning, cancelling any
    DOOR_NOT_OPENED countdown that may be running.
    """
    global _door_left_open_timer, _door_opened_this_morning

    _door_opened_this_morning = True
    cancel_door_not_opened_timer('door opened')

    if _door_left_open_timer is not None:
        return  # Timer already running — door was already open

    band = sal_state.current_time_band()
    key = ('DOOR_LEFT_OPEN', None)
    if key not in sal_state.EVENT_THRESHOLDS:
        print('DOOR_LEFT_OPEN: no threshold configured — timer not started')
        return

    threshold_sec = sal_state.EVENT_THRESHOLDS[key][band]['yellow_sec']
    print(f'DOOR_LEFT_OPEN timer started — {threshold_sec}s')
    _door_left_open_timer = threading.Timer(
        threshold_sec, _fire_door_left_open
    )
    _door_left_open_timer.start()


def on_door_closed():
    """
    Called by sal.py on_message when the entrance door closes (contact=True).
    Cancels the DOOR_LEFT_OPEN timer — door closed before it fired.
    """
    global _door_left_open_timer

    if _door_left_open_timer is not None:
        _door_left_open_timer.cancel()
        _door_left_open_timer = None
        print('DOOR_LEFT_OPEN timer cancelled — door closed')


def _fire_door_left_open():
    """
    Called when the DOOR_LEFT_OPEN timer expires. The door is still open.
    Raises the clinical alert.
    """
    global _door_left_open_timer
    _door_left_open_timer = None

    resident_info = sal_db.load_resident_info(UNIT)
    if resident_info is None:
        print('DOOR_LEFT_OPEN fired: no Active resident — raising technical alert')
        sal_db.insert_technical_alert(UNIT, 'resident_data_missing', 'orange')
        return

    print('DOOR_LEFT_OPEN confirmed — raising alert')
    sal_db.insert_clinical_alert(
        UNIT,
        resident_info['resident_uuid'],
        'door_left_open_yellow',
        'yellow'
    )


# ---------------------------------------------------------------------------
# DOOR_NOT_OPENED
# ---------------------------------------------------------------------------

def start_door_not_opened_timer():
    """
    Called by sal_loop.py at the first loop tick after the morning time band
    begins. Starts the DOOR_NOT_OPENED countdown. If a timer is already
    running from a previous call (loop ticked twice in the morning band
    before the door opened), the existing timer continues unchanged.

    Resets _door_opened_this_morning to False — a new morning band begins.
    """
    global _door_not_opened_timer, _door_opened_this_morning

    _door_opened_this_morning = False

    if _door_not_opened_timer is not None:
        return  # Already running

    key = ('DOOR_NOT_OPENED', None)
    if key not in sal_state.EVENT_THRESHOLDS:
        print('DOOR_NOT_OPENED: no threshold configured — timer not started')
        return

    band = sal_state.current_time_band()
    threshold_sec = sal_state.EVENT_THRESHOLDS[key][band]['yellow_sec']
    print(f'DOOR_NOT_OPENED timer started — {threshold_sec}s')
    _door_not_opened_timer = threading.Timer(
        threshold_sec, _fire_door_not_opened
    )
    _door_not_opened_timer.start()


def cancel_door_not_opened_timer(reason='door opened'):
    """
    Cancel the DOOR_NOT_OPENED timer. Called when the door opens during
    the morning countdown — normal morning activity, no alert needed.
    """
    global _door_not_opened_timer

    if _door_not_opened_timer is not None:
        _door_not_opened_timer.cancel()
        _door_not_opened_timer = None
        print(f'DOOR_NOT_OPENED timer cancelled — {reason}')


def _fire_door_not_opened():
    """
    Called when the DOOR_NOT_OPENED timer expires. The door has not opened
    since the morning band started. Raises the clinical alert.
    """
    global _door_not_opened_timer
    _door_not_opened_timer = None

    resident_info = sal_db.load_resident_info(UNIT)
    if resident_info is None:
        print('DOOR_NOT_OPENED fired: no Active resident — raising technical alert')
        sal_db.insert_technical_alert(UNIT, 'resident_data_missing', 'orange')
        return

    print('DOOR_NOT_OPENED confirmed — raising alert')
    sal_db.insert_clinical_alert(
        UNIT,
        resident_info['resident_uuid'],
        'door_not_opened_yellow',
        'yellow'
    )
