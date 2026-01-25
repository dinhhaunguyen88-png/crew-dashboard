"""
Models Package - Data Models and Interfaces
"""

from models.crew import (
    CrewRole,
    DutyStatus,
    AlertLevel,
    CrewMember,
    CrewRollingHours,
    CrewScheduleRecord
)

from models.flight import (
    Flight,
    FlightLeg,
    FlightStatus
)

__all__ = [
    'CrewRole',
    'DutyStatus', 
    'AlertLevel',
    'CrewMember',
    'CrewRollingHours',
    'CrewScheduleRecord',
    'Flight',
    'FlightLeg',
    'FlightStatus'
]
