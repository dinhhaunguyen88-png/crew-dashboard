"""
Crew Data Models

Defines data structures for crew-related entities.
Used across CSV, AIMS, and Supabase integrations.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from datetime import datetime, date
from enum import Enum


class CrewRole(Enum):
    """Crew position/role codes"""
    CP = "Captain"
    FO = "First Officer"
    PU = "Purser"
    FA = "Flight Attendant"
    CA = "Cabin Attendant"
    
    @classmethod
    def from_string(cls, value: str) -> 'CrewRole':
        """Parse role from string, default to FA"""
        try:
            return cls[value.upper()]
        except (KeyError, AttributeError):
            return cls.FA


class DutyStatus(Enum):
    """Crew duty status codes"""
    OPERATING = "Operating"
    STANDBY = "SBY"
    OFFICE_STANDBY = "OSBY"
    SICK_LEAVE = "SL"
    CALL_SICK = "CSL"
    FATIGUE = "FTG"
    OFF = "OFF"
    TRAINING = "TRN"
    UNKNOWN = "UNK"
    
    @classmethod
    def from_string(cls, value: str) -> 'DutyStatus':
        """Parse status from string"""
        if not value:
            return cls.UNKNOWN
        
        value_upper = value.upper().strip()
        
        # Direct match
        try:
            return cls[value_upper]
        except KeyError:
            pass
        
        # Value match
        for status in cls:
            if status.value.upper() == value_upper:
                return status
        
        return cls.UNKNOWN


class AlertLevel(Enum):
    """Block hours alert levels per Alert Matrix"""
    NORMAL = "normal"      # <= 85 hours (28-day)
    WARNING = "warning"    # > 85 hours 
    CRITICAL = "critical"  # > 95 hours
    
    @staticmethod
    def from_hours(hours_28day: float) -> 'AlertLevel':
        """Determine alert level from 28-day block hours"""
        if hours_28day > 95:
            return AlertLevel.CRITICAL
        elif hours_28day > 85:
            return AlertLevel.WARNING
        return AlertLevel.NORMAL


@dataclass
class CrewMember:
    """Crew member entity"""
    crew_id: str
    name: str
    role: CrewRole
    base: Optional[str] = None
    ac_type: Optional[str] = None
    seniority: Optional[str] = None
    email: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CrewMember':
        """Create from dictionary"""
        return cls(
            crew_id=str(data.get('crew_id', data.get('id', ''))),
            name=data.get('name', ''),
            role=CrewRole.from_string(data.get('role', 'FA')),
            base=data.get('base'),
            ac_type=data.get('ac_type'),
            seniority=data.get('seniority'),
            email=data.get('email')
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'crew_id': self.crew_id,
            'name': self.name,
            'role': self.role.name,
            'base': self.base,
            'ac_type': self.ac_type,
            'seniority': self.seniority,
            'email': self.email
        }


@dataclass
class CrewRollingHours:
    """Crew rolling block hours record"""
    crew_id: str
    name: str
    hours_28day: float
    hours_12month: float
    percentage_28day: float
    status: AlertLevel
    seniority: Optional[str] = None
    last_updated: datetime = field(default_factory=datetime.now)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CrewRollingHours':
        """Create from dictionary"""
        hours_28 = float(data.get('hours_28day', 0))
        return cls(
            crew_id=str(data.get('crew_id', data.get('id', ''))),
            name=data.get('name', ''),
            hours_28day=hours_28,
            hours_12month=float(data.get('hours_12month', 0)),
            percentage_28day=float(data.get('percentage', 0)),
            status=AlertLevel.from_hours(hours_28),
            seniority=data.get('seniority')
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'crew_id': self.crew_id,
            'name': self.name,
            'hours_28day': self.hours_28day,
            'hours_12month': self.hours_12month,
            'percentage': self.percentage_28day,
            'status': self.status.value,
            'seniority': self.seniority,
            'last_updated': self.last_updated.isoformat()
        }
    
    @staticmethod
    def calculate_status(hours_28day: float) -> AlertLevel:
        """Determine alert status based on 28-day hours"""
        return AlertLevel.from_hours(hours_28day)


@dataclass
class CrewScheduleRecord:
    """Individual crew schedule/duty record"""
    crew_id: str
    crew_name: str
    duty_date: date
    duty_type: DutyStatus
    base: Optional[str] = None
    ac_type: Optional[str] = None
    position: Optional[str] = None
    remarks: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CrewScheduleRecord':
        """Create from dictionary"""
        # Parse date
        duty_date_val = data.get('duty_date', data.get('date'))
        if isinstance(duty_date_val, str):
            try:
                duty_date_val = datetime.strptime(duty_date_val, '%Y-%m-%d').date()
            except ValueError:
                try:
                    duty_date_val = datetime.strptime(duty_date_val, '%d/%m/%y').date()
                except ValueError:
                    duty_date_val = date.today()
        elif isinstance(duty_date_val, datetime):
            duty_date_val = duty_date_val.date()
        elif duty_date_val is None:
            duty_date_val = date.today()
        
        return cls(
            crew_id=str(data.get('crew_id', '')),
            crew_name=data.get('crew_name', data.get('name', '')),
            duty_date=duty_date_val,
            duty_type=DutyStatus.from_string(data.get('duty_type', data.get('status_type', ''))),
            base=data.get('base'),
            ac_type=data.get('ac_type'),
            position=data.get('position'),
            remarks=data.get('remarks')
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'crew_id': self.crew_id,
            'crew_name': self.crew_name,
            'duty_date': self.duty_date.isoformat() if self.duty_date else None,
            'duty_type': self.duty_type.value,
            'base': self.base,
            'ac_type': self.ac_type,
            'position': self.position,
            'remarks': self.remarks
        }


@dataclass
class CrewScheduleSummary:
    """Summary of crew schedule statuses for a date"""
    date: Optional[date]
    standby_count: int = 0
    office_standby_count: int = 0
    sick_leave_count: int = 0
    call_sick_count: int = 0
    fatigue_count: int = 0
    operating_count: int = 0
    
    @property
    def total_unavailable(self) -> int:
        """Total unavailable crew"""
        return self.sick_leave_count + self.call_sick_count + self.fatigue_count
    
    @property
    def total_standby(self) -> int:
        """Total standby crew"""
        return self.standby_count + self.office_standby_count
    
    def to_dict(self) -> Dict[str, int]:
        """Convert to dictionary matching legacy format"""
        return {
            'SBY': self.standby_count,
            'OSBY': self.office_standby_count,
            'SL': self.sick_leave_count,
            'CSL': self.call_sick_count,
            'FTG': self.fatigue_count
        }
