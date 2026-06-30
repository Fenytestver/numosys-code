"""
sal_silence.py — WHOLE_APARTMENT_SILENT confirmation timer
Nomusys elder care monitoring system, CT 106

T-SAL2 increment 3 (revised design — June 24, 2026).

WHOLE_APARTMENT_SILENT is an immediate-red emergency: no sensor anywhere in
the unit has detected presence or motion. This module implements the
confirmation timer that gates the alert.

Design (see flowchart in session log, June 24, 2026):

False message path (called from sal.py on_message after every False):
  1. If unit_silent is already True — timer already running, do nothing.
  2. If unit_silent is False — do nothing (update_unit_states already set
     it; if it's still False, at least one room is still occupied).
  3. If unit_silent just became True (set by update_unit_states) — start
     the confirmation timer (red_sec from sal_event_thresholds).
  4. Timer fires without being cancelled — issue the alert.

True message path (called from sal.py on_message after every True):
  1. If unit_silent is already False — apartment was not silent, nothing
     to cancel, do nothing.
  2. If unit_silent is True — presence detected while timer was running.
     Cancel the timer, auto-clear any open alert.
     (update_unit_states has already set unit_silent = False by the time
     this is called.)

The timer is not cancelled by update_unit_states directly — sal_silence.py
owns the timer and is the only place it is started or cancelled. This keeps
the timer lifecycle in one place.

unit_silent lives in sal_state.py alongside ROOM_STATE and is maintained
by update_unit_states. sal_silence.py reads it but never writes it.
"""

import threading
import sal_db
import sal_state
from sal_config import UNIT


# Module-level timer slot. None means no timer is running.
_silence_timer = None


def on_false_message():
    """
    Called from sal.py on_message after every PIR/presence False message,
    after update_unit_states has run.

    Checks unit_silent. If True and no timer is running, starts the
    confirmation timer. If unit_silent is False, does nothing.

    Skipped entirely while the unit is in manual mode (Sensor Availability,
    T-SAL2 increment 4) — manual mode blocks new timer starts. See
    sal_availability.py.
    """
    global _silence_timer

    import sal_availability
    if sal_availability.unit_manual_mode:
        return

    if not sal_state.unit_silent:
        return  # At least one room still occupied — nothing to do

    if _silence_timer is not None:
        return  # Timer already running — condition already detected

    # Don't restart the cycle if the alert is already open.
    if sal_db.find_open_alerts(UNIT, ['whole_apartment_silent_red']):
        return

    key = ('WHOLE_APARTMENT_SILENT', None)
    if key not in sal_state.EVENT_THRESHOLDS:
        print('WHOLE_APARTMENT_SILENT: no threshold configured — timer not started')
        return

    band = sal_state.current_time_band()
    threshold_sec = sal_state.EVENT_THRESHOLDS[key][band]['red_sec']
    print(f'WHOLE_APARTMENT_SILENT: unit silent — starting confirmation timer ({threshold_sec}s)')
    _silence_timer = threading.Timer(threshold_sec, _fire_alert)
    _silence_timer.start()


def on_true_message():
    """
    Called from sal.py on_message after every PIR/presence True message,
    after update_unit_states has run.

    Always cancels the confirmation timer if one is running, and auto-clears
    any open alert. Does not check unit_silent — by the time this is called,
    update_unit_states has already set unit_silent to False. The timer
    running is itself the signal that cancellation is needed.
    """
    global _silence_timer

    if _silence_timer is not None:
        _silence_timer.cancel()
        _silence_timer = None
        print('WHOLE_APARTMENT_SILENT: presence detected — timer cancelled')

    open_alerts = sal_db.find_open_alerts(UNIT, ['whole_apartment_silent_red'])
    for row in open_alerts:
        sal_db.auto_clear_alert(row['alert_id'])
        print(f'WHOLE_APARTMENT_SILENT: auto-cleared alert {row["alert_id"]}')


def _fire_alert():
    """
    Called when the confirmation timer expires without being cancelled.
    Timer not cancelled = unit_silent was never cleared = apartment is
    still silent. Issue the alert.
    """
    global _silence_timer
    _silence_timer = None

    resident_info = sal_db.load_resident_info(UNIT)
    if resident_info is None:
        print('WHOLE_APARTMENT_SILENT fired: no Active resident — raising technical alert')
        sal_db.insert_technical_alert(UNIT, 'resident_data_missing', 'orange')
        return

    print('WHOLE_APARTMENT_SILENT confirmed — raising alert')
    sal_db.insert_clinical_alert(
        UNIT,
        resident_info['resident_uuid'],
        'whole_apartment_silent_red',
        'red'
    )


def cancel(reason='SAL shutting down'):
    """
    Cancel the confirmation timer if running. Called on shutdown.
    """
    global _silence_timer
    if _silence_timer is not None:
        _silence_timer.cancel()
        _silence_timer = None
        print(f'WHOLE_APARTMENT_SILENT timer cancelled — {reason}')
