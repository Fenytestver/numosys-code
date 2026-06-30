"""
sal_exit_arrival.py — EXIT and ARRIVAL recognition (D20)
Nomusys elder care monitoring system, CT 106

T-SAL2 increment 2. Replaces the EXIT/ENTRY logic that lived in sal.py
(all_pir_silent, confirm_exit, start_exit_window, cancel_exit_window,
publish_entry) with the D20 redesign:

EXIT     — unchanged trigger (entrance door opens then closes), but
            confirmation now uses unit_appears_empty() — the Per-Room
            State Model, covering both PIR and presence sensors — instead
            of the old PIR-only all_pir_silent(). EXIT confirmation also
            now runs the Compound EXIT Evaluation (mobility classification
            + time band -> IMMOBILE_RESIDENT_EXIT / MOBILE_WITH_ASSISTANCE_
            NIGHT_EXIT / NIGHT_EXIT / nothing) and raises the matching
            clinical alert.

ARRIVAL  — no longer a door-sequence event (replaces the old ENTRY).
            ARRIVAL is the state transition AWAY_INFERRED -> IN_ROOM
            itself, regardless of which sensor or sequence caused it.
            Recognised here by write_presence_status(), the single
            function every caller in this file and in sal.py must go
            through to write resident_presence.status — this guarantees
            the transition is checked in exactly one place. On ARRIVAL,
            auto-clears any open RETURN_OVERDUE and compound-EXIT alerts.

See nomusys_design.md, Situational Awareness Layer > EXIT and ARRIVAL
Recognition Detail, and Compound EXIT Evaluation.
"""

import json
import threading
from datetime import datetime, timezone

import sal_db
import sal_state
from sal_config import SILENCE_WINDOW_SEC, UNIT
from sal_state import state


# Alert reason codes auto-cleared on ARRIVAL — RETURN_OVERDUE (both
# tiers) plus all three compound EXIT alerts. See nomusys_design.md,
# ARRIVAL row in the Event Library table ("auto-resolves RETURN_OVERDUE
# and all open exit-related alerts").
EXIT_RELATED_ALERT_CODES = [
    'return_overdue_yellow',
    'return_overdue_orange',
    'immobile_resident_exit_red',
    'mobile_with_assistance_night_exit_orange',
    'night_exit_yellow',
]


# ---------------------------------------------------------------------------
# PRESENCE STATUS WRITE + ARRIVAL RECOGNITION
# Every write to resident_presence.status in this file and in sal.py goes
# through this one function, so the AWAY_INFERRED -> IN_ROOM transition
# (ARRIVAL) is only ever checked in one place.
# ---------------------------------------------------------------------------

def write_presence_status(client, new_status):
    """
    Write resident_presence.status via sal_db.set_presence_status, then
    check whether this write is the specific transition that defines
    ARRIVAL (D20): previous status was AWAY_INFERRED, new status is
    IN_ROOM. If so, publish ARRIVAL and auto-clear the exit-related
    alerts.

    Reads state['presence_status'] for the previous value before calling
    set_presence_status (which may update it), so the comparison is
    against what was true a moment ago, not the just-written value.

    Returns the new presence_status value (same return contract as
    set_presence_status) so callers can store it back into state exactly
    as they did before.
    """
    previous_status = state['presence_status']
    updated_status = sal_db.set_presence_status(UNIT, new_status, previous_status)

    if previous_status == 'AWAY_INFERRED' and updated_status == 'IN_ROOM':
        publish_arrival(client)

    return updated_status


def publish_arrival(client):
    """
    Publish ARRIVAL and auto-clear RETURN_OVERDUE and the compound EXIT
    alerts. Called only from write_presence_status, only on the specific
    AWAY_INFERRED -> IN_ROOM transition — not on every sensor message
    while already IN_ROOM, consistent with set_presence_status's existing
    write-deduplication rule.

    IN_ROOM makes no claim about who triggered it — resident, nurse,
    cleaner, or visitor all produce the same sensor pattern. A single
    PIR or presence firing is treated as a full return; the Bayesian-
    engine-side false-positive nuance is not evaluated while the engine
    is dormant (D19) — see nomusys_design.md, AWAY_INFERRED and IN_ROOM
    Ownership.
    """
    print('ARRIVAL confirmed — publishing')
    payload = json.dumps({
        'event': 'ARRIVAL',
        'unit': UNIT,
        'ts': datetime.now(timezone.utc).isoformat()
    })
    client.publish(f'nomusys/situational/{UNIT}', payload)

    open_alerts = sal_db.find_open_alerts(UNIT, EXIT_RELATED_ALERT_CODES)
    for alert in open_alerts:
        sal_db.auto_clear_alert(alert['alert_id'])
        print(f"Auto-cleared on ARRIVAL: {alert['reason_code']} (id={alert['alert_id']})")


# ---------------------------------------------------------------------------
# EXIT EVENT LOGIC (D20)
# Trigger unchanged: entrance door opens, then closes. Confirmation now
# uses unit_appears_empty() (Per-Room State Model — PIR and presence)
# instead of the old PIR-only all_pir_silent().
# ---------------------------------------------------------------------------

def start_exit_window(client):
    """
    Start the confirmation window countdown after the entrance door
    closes. Same double-start guard as the previous version: if a window
    is already active, the existing window continues.

    SILENCE_WINDOW_SEC is the lab-phase hardcoded duration (sal_config.py).
    Production reads exit_confirmation_window_sec per time band from
    EVENT_THRESHOLDS instead — see nomusys_design.md, EXIT and ARRIVAL
    Recognition Detail. Not changed in this increment; the window
    duration source is a separate, later concern from the confirmation
    *logic* itself.

    Skipped entirely while the unit is in manual mode (Sensor Availability,
    T-SAL2 increment 4) — manual mode blocks new timer starts. EXIT itself
    will not be confirmed or published while the unit is in manual mode.
    See sal_availability.py.
    """
    import sal_availability
    if sal_availability.unit_manual_mode:
        return

    if state['exit_window_active']:
        return
    
    print(f'EXIT window started — {SILENCE_WINDOW_SEC}s')
    state['exit_window_active'] = True
    t = threading.Timer(SILENCE_WINDOW_SEC, confirm_exit, args=[client])
    state['exit_window_timer'] = t
    t.start()


def cancel_exit_window(reason='activity detected'):
    """
    Cancel the active confirmation window countdown. Unchanged from the
    previous version — called when PIR/presence activity is detected
    during the window, or when the entrance door opens again.
    """
    was_active = state['exit_window_active']
    if state['exit_window_timer']:
        state['exit_window_timer'].cancel()
        state['exit_window_timer'] = None
    state['exit_window_active'] = False
    if was_active:
        print(f'EXIT window cancelled — {reason}')


def confirm_exit(client):
    """
    Called when the confirmation window timer expires.

    Re-checks unit_appears_empty() at the moment of expiry (a sensor
    event could have arrived between the timer firing and this function
    executing — same reasoning as the previous version's all_pir_silent()
    re-check). Only confirms EXIT if the unit still appears empty.

    On confirmation: writes AWAY_INFERRED, publishes EXIT, then runs the
    Compound EXIT Evaluation. EXIT publishes regardless of what the
    compound evaluation finds (or fails to find) — the named event does
    not depend on resident data; only the extra alert decision does.
    """
    state['exit_window_active'] = False
    state['exit_window_timer'] = None

    if not sal_state.unit_appears_empty():
        print('EXIT window expired — unit not empty, not confirmed')
        return

    print('EXIT confirmed — publishing')
    state['presence_status'] = write_presence_status(client, 'AWAY_INFERRED')
    payload = json.dumps({
        'event': 'EXIT',
        'unit': UNIT,
        'ts': datetime.now(timezone.utc).isoformat()
    })
    client.publish(f'nomusys/situational/{UNIT}', payload)

    evaluate_compound_exit()


# ---------------------------------------------------------------------------
# COMPOUND EXIT EVALUATION (D20)
# IMMOBILE_RESIDENT_EXIT, MOBILE_WITH_ASSISTANCE_NIGHT_EXIT, NIGHT_EXIT —
# three different clinical interpretations of the same EXIT-confirmation
# moment, not three independently-triggered events. Priority order, first
# match wins. See nomusys_design.md, Compound EXIT Evaluation.
# ---------------------------------------------------------------------------

def evaluate_compound_exit():
    """
    Run immediately after EXIT confirms. Looks up the unit's Active
    resident (mobility_classification, resident_uuid) and the current time
    band, then applies the priority-ordered rule:

      1. Immobile                                  -> red   (IMMOBILE_RESIDENT_EXIT)
      2. Mobile_with_assistance AND night           -> orange (MOBILE_WITH_ASSISTANCE_NIGHT_EXIT)
      3. night (any other mobility classification)  -> yellow (NIGHT_EXIT)
      4. otherwise                                   -> no alert

    Missing resident data (decided June 17, 2026): if load_resident_info
    finds no Active resident, this is treated as a data-entry/setup
    fault, not a transient runtime condition to guess around. Raises a
    technical alert (resident_data_missing, orange) and stops — no
    compound alert is raised, no severity is guessed, and the unit is
    NOT put into manual mode. EXIT has already published and
    AWAY_INFERRED has already been written by the caller, regardless of
    this evaluation's outcome. The durable fix is a daily data integrity
    check (separate backlog item), not defensive code here.
    """
    resident_info = sal_db.load_resident_info(UNIT)
    if resident_info is None:
        print('Compound EXIT evaluation: no Active resident found for unit — '
              'raising resident_data_missing technical alert, skipping evaluation')
        sal_db.insert_technical_alert(UNIT, 'resident_data_missing', 'orange')
        return

    mobility = resident_info['mobility_classification']
    resident_uuid = resident_info['resident_uuid']
    band = sal_state.current_time_band()
    is_night = (band == 'night')

    if mobility == 'Immobile':
        sal_db.insert_clinical_alert(UNIT, resident_uuid, 'immobile_resident_exit_red', 'red')
    elif mobility == 'Mobile_with_assistance' and is_night:
        sal_db.insert_clinical_alert(UNIT, resident_uuid, 'mobile_with_assistance_night_exit_orange', 'orange')
    elif is_night:
        sal_db.insert_clinical_alert(UNIT, resident_uuid, 'night_exit_yellow', 'yellow')
    else:
        print('Compound EXIT evaluation: no alert condition met')
