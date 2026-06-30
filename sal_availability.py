"""
sal_availability.py — Sensor Availability / unit-wide manual mode
Nomusys elder care monitoring system, CT 106

T-SAL2 increment 4 (design session June 25, 2026).

One fact per sensor, published by Node-RED on
nomusys/availability/[device], payload {"device": ..., "available": bool}.
Node-RED detects availability via Z2M's availability feature (enabled
for both mains and battery-powered devices) — see nomusys_design.md
Sensor Availability (D20, revised).

Any sensor in the unit going unavailable puts the whole unit into manual
mode: all clinical evaluation suspends — no new alerts, no new timers —
until every sensor in the unit is available again. Sensor messages still
update the Per-Room State Model while suspended (see sal_state.py); the
SAL is not blind, just quiet.

unit_manual_mode lives here, not in sal_state.py — it is read by every
other SAL module before starting a timer or raising an alert. Modules
check sal_availability.unit_manual_mode directly.

Auto-clears triggered by genuine sensor events (ARRIVAL, door close,
presence clearing) still run during manual mode — manual mode blocks
new alert creation and new timer starts, not resolution of alerts
already open when the underlying facts change. See nomusys_design.md
Sensor Availability for the full statement of this principle.

sensor_unavailable_red (technical alert, maintenance) is raised and
cleared by Node-RED directly, per D15 — detection and alerting
co-located. This module does not touch that alert.

unit_monitoring_suspended_orange (clinical alert, nursing) is raised
and cleared by this module.
"""

import sal_db
from sal_config import UNIT


# Set of device names currently reporting unavailable for this unit.
# Empty set means every known sensor in the unit is available.
_unavailable_sensors = set()

# True whenever _unavailable_sensors is non-empty. Read by other SAL
# modules before starting a timer or raising an alert.
unit_manual_mode = False


def on_availability_message(device, available):
    """
    Called from sal.py on_message for every message on
    nomusys/availability/[device].

    Updates the unavailable-sensor set for the unit and enters or exits
    manual mode as needed.
    """
    global unit_manual_mode

    was_manual_mode = unit_manual_mode

    if available:
        _unavailable_sensors.discard(device)
    else:
        _unavailable_sensors.add(device)

    unit_manual_mode = len(_unavailable_sensors) > 0

    if unit_manual_mode and not was_manual_mode:
        _enter_manual_mode()
    elif was_manual_mode and not unit_manual_mode:
        _exit_manual_mode()


def _enter_manual_mode():
    """
    First sensor in the unit just went unavailable. Cancel all running
    timers across every SAL module and raise the nursing alert.

    Open clinical alerts already raised before manual mode started are
    left untouched — they remain valid and still need a nurse response.
    """
    print('Sensor Availability: unit entering manual mode — '
          f'unavailable: {sorted(_unavailable_sensors)}')

    _cancel_all_timers('unit entered manual mode')

    resident_info = sal_db.load_resident_info(UNIT)
    if resident_info is None:
        print('Sensor Availability: no Active resident — '
              'raising technical alert instead')
        sal_db.insert_technical_alert(UNIT, 'resident_data_missing', 'orange')
        return

    sal_db.insert_clinical_alert(
        UNIT,
        resident_info['resident_uuid'],
        'unit_monitoring_suspended_orange',
        'orange'
    )


def _exit_manual_mode():
    """
    Last unavailable sensor in the unit has recovered. Auto-clear the
    nursing alert. Clinical evaluation resumes immediately — no other
    action needed, since the Per-Room State Model has stayed current
    throughout manual mode.
    """
    print('Sensor Availability: all sensors available — exiting manual mode')

    open_alerts = sal_db.find_open_alerts(
        UNIT, ['unit_monitoring_suspended_orange']
    )
    for alert in open_alerts:
        sal_db.auto_clear_alert(alert['alert_id'])
        print(f'Sensor Availability: auto-cleared alert {alert["alert_id"]}')


def _cancel_all_timers(reason):
    """
    Cancel every running timer in every SAL module that owns one.
    Called once, when the unit enters manual mode.

    sal_loop.py owns no cancellable timer of its own — it only runs a
    fixed-period sleep loop and signals day/night boundaries to
    sal_door, so it is not called here.

    Each module's cancel function leaves its own state consistent —
    the same functions each module already calls on SAL shutdown or
    on its own normal cancellation paths.
    """
    import sal_silence
    import sal_door
    import sal_exit_arrival

    sal_silence.cancel(reason)
    sal_door.cancel_door_not_opened_timer(reason)
    sal_door.cancel_door_left_open_timer(reason)
    sal_exit_arrival.cancel_exit_window(reason)