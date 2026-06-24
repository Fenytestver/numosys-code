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
    Alert   : door_left_open_yellow (single tier — yellow only).

DOOR_NOT_OPENED
    Trigger : sal_loop.py starts the timer at the night→day band transition.
              sal_loop.py cancels the timer at the day→night transition.
    Mechanism: while the daytime window is active, every door-close starts
               a countdown. If the door does not open again before the timer
               fires, a clinical alert is raised. Every door-open cancels the
               countdown; every subsequent door-close restarts it — so a nurse
               visit resets the clock, and the resident is monitored throughout
               the day, not just from morning start.
    Cancel  : door opens (contact=False) during the countdown.
    Night   : sal_loop cancels any running timer at the day→night transition.
              Door-close events during night band are ignored for this event.
    Alert   : door_not_opened_yellow (single tier — yellow only).

Boundary responsibility:
    sal_loop  — owns the day/night boundary: starts at night→day transition,
                cancels at day→night transition.
    sal_door  — owns what happens while the daytime window is active: cancels
                on door-open, restarts on door-close.

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

import sal_db
import sal_state
from sal_config import UNIT


# ---------------------------------------------------------------------------
# MODULE-LEVEL TIMER STATE
# One slot per event type. None means no timer is currently running.
# ---------------------------------------------------------------------------

_door_left_open_timer = None
_door_not_opened_timer = None

# Set to True by sal_loop when the daytime window is active (night→day
# transition). Set to False at day→night transition. sal_door uses this
# to decide whether to start a DOOR_NOT_OPENED countdown on door-close.
_daytime_window_active = False


# ---------------------------------------------------------------------------
# DOOR_LEFT_OPEN
# ---------------------------------------------------------------------------

def on_door_opened():
    """
    Called by sal.py on_message when the entrance door opens (contact=False).

    Starts the DOOR_LEFT_OPEN countdown timer. If a timer is already running
    (door opened twice without closing — shouldn't happen in practice but
    guarded against), the existing timer continues unchanged.

    Also cancels any running DOOR_NOT_OPENED countdown — the door has opened,
    which is exactly the normal daytime activity the timer was waiting for.
    The next door-close will restart the DOOR_NOT_OPENED countdown.
    """
    global _door_left_open_timer

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

    Also starts the DOOR_NOT_OPENED countdown if the daytime window is active
    (sal_loop has signalled that we are in a non-night band). If the daytime
    window is not active (night band), the door-close is ignored for
    DOOR_NOT_OPENED purposes.
    """
    global _door_left_open_timer

    if _door_left_open_timer is not None:
        _door_left_open_timer.cancel()
        _door_left_open_timer = None
        print('DOOR_LEFT_OPEN timer cancelled — door closed')

    if _daytime_window_active:
        _start_door_not_opened_timer()


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

def start_daytime_window():
    """
    Called by sal_loop.py at the night→day band transition.
    Activates the daytime monitoring window and starts the DOOR_NOT_OPENED
    countdown — the door has not opened yet since daytime began.
    """
    global _daytime_window_active
    _daytime_window_active = True
    print('DOOR_NOT_OPENED: daytime window started')
    _start_door_not_opened_timer()


def end_daytime_window():
    """
    Called by sal_loop.py at the day→night band transition.
    Deactivates the daytime monitoring window and cancels any running
    DOOR_NOT_OPENED countdown — night band, no monitoring needed.
    """
    global _daytime_window_active
    _daytime_window_active = False
    cancel_door_not_opened_timer('night band started')


def _start_door_not_opened_timer():
    """
    Internal. Start the DOOR_NOT_OPENED countdown using the current band's
    threshold. If a timer is already running, it continues unchanged — this
    guards against the loop ticking twice in the same band before the door
    opens.
    """
    global _door_not_opened_timer

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
    Cancel the DOOR_NOT_OPENED timer. Called when the door opens (normal
    daytime activity) or when the night band starts (sal_loop boundary).
    Safe to call when no timer is running.
    """
    global _door_not_opened_timer

    if _door_not_opened_timer is not None:
        _door_not_opened_timer.cancel()
        _door_not_opened_timer = None
        print(f'DOOR_NOT_OPENED timer cancelled — {reason}')


def _fire_door_not_opened():
    """
    Called when the DOOR_NOT_OPENED timer expires. The door has not opened
    within the threshold period. Raises the clinical alert.
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
