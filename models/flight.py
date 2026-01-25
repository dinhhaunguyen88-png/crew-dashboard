"""
Flight Data Models

Defines data structures for flight-related entities.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from datetime import datetime, date, time
from enum import Enum


class FlightStatus(Enum):
    """Flight operational status"""
    SCHEDULED = "SCH"
    DEPARTED = "DEP"
    ARRIVED = "ARR"
    CANCELLED = "CNL"
    DELAYED = "DLY"
    DIVERTED = "DIV"
    
    @classmethod
    def from_string(cls, value: str) -> 'FlightStatus':
        """Parse status from string"""
        if not value:
            return cls.SCHEDULED
        try:
            return cls[value.upper()]
        except KeyError:
            return cls.SCHEDULED


@dataclass
class Flight:
    """Flight record from DayRepReport"""
    date: str                    # Operating date (DD/MM/YY)
    calendar_date: str           # Calendar date
    reg: str                     # Aircraft registration
    flt: str                     # Flight number
    dep: str                     # Departure airport
    arr: str                     # Arrival airport
    std: str                     # Scheduled Time of Departure (HH:MM)
    sta: str                     # Scheduled Time of Arrival (HH:MM)
    crew: Optional[str] = None   # Crew string
    status: FlightStatus = FlightStatus.SCHEDULED
    block_minutes: Optional[int] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Flight':
        """Create from dictionary"""
        return cls(
            date=data.get('date', ''),
            calendar_date=data.get('calendar_date', ''),
            reg=data.get('reg', ''),
            flt=data.get('flt', ''),
            dep=data.get('dep', ''),
            arr=data.get('arr', ''),
            std=data.get('std', ''),
            sta=data.get('sta', ''),
            crew=data.get('crew'),
            status=FlightStatus.from_string(data.get('status', '')),
            block_minutes=data.get('block_minutes')
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'date': self.date,
            'calendar_date': self.calendar_date,
            'reg': self.reg,
            'flt': self.flt,
            'dep': self.dep,
            'arr': self.arr,
            'std': self.std,
            'sta': self.sta,
            'crew': self.crew,
            'status': self.status.value,
            'block_minutes': self.block_minutes
        }
    
    def calculate_block_minutes(self) -> Optional[int]:
        """Calculate block time in minutes from STD/STA"""
        try:
            if not self.std or not self.sta:
                return None
            
            std_parts = self.std.split(':')
            sta_parts = self.sta.split(':')
            
            std_min = int(std_parts[0]) * 60 + int(std_parts[1])
            sta_min = int(sta_parts[0]) * 60 + int(sta_parts[1])
            
            duration = sta_min - std_min
            if duration < 0:
                duration += 24 * 60  # Handle overnight flights
            
            return duration
        except (ValueError, IndexError):
            return None


@dataclass
class FlightLeg:
    """
    Flight leg with actual times (from AIMS)
    
    Represents a single leg of a flight with actual departure/arrival times.
    Used for calculating actual block hours.
    """
    leg_id: str
    flight_number: str
    date: date
    departure_airport: str
    arrival_airport: str
    aircraft_reg: str
    
    # Scheduled times
    std: Optional[time] = None
    sta: Optional[time] = None
    
    # Actual times (from AIMS)
    atd: Optional[time] = None  # Actual Time of Departure
    ata: Optional[time] = None  # Actual Time of Arrival
    
    # Calculated values
    block_minutes: Optional[int] = None
    
    # Crew assignments
    crew_ids: List[str] = field(default_factory=list)
    
    @classmethod
    def from_aims_response(cls, data: Dict[str, Any]) -> 'FlightLeg':
        """Create from AIMS API response"""
        # Parse date
        leg_date = data.get('date')
        if isinstance(leg_date, str):
            try:
                leg_date = datetime.strptime(leg_date, '%Y-%m-%d').date()
            except ValueError:
                leg_date = date.today()
        
        return cls(
            leg_id=str(data.get('leg_id', data.get('nLegId', ''))),
            flight_number=data.get('flight_number', data.get('cFlightNumber', '')),
            date=leg_date,
            departure_airport=data.get('dep', data.get('cDepAirportCode', '')),
            arrival_airport=data.get('arr', data.get('cArrAirportCode', '')),
            aircraft_reg=data.get('reg', data.get('cAircraftReg', '')),
            block_minutes=data.get('block_minutes', data.get('nBlockTime'))
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'leg_id': self.leg_id,
            'flight_number': self.flight_number,
            'date': self.date.isoformat() if self.date else None,
            'departure_airport': self.departure_airport,
            'arrival_airport': self.arrival_airport,
            'aircraft_reg': self.aircraft_reg,
            'std': self.std.isoformat() if self.std else None,
            'sta': self.sta.isoformat() if self.sta else None,
            'atd': self.atd.isoformat() if self.atd else None,
            'ata': self.ata.isoformat() if self.ata else None,
            'block_minutes': self.block_minutes,
            'crew_ids': self.crew_ids
        }


@dataclass
class AircraftUtilization:
    """Aircraft utilization statistics"""
    date: str
    ac_type: str
    dom_block: str = "00:00"      # Domestic block hours
    int_block: str = "00:00"      # International block hours
    total_block: str = "00:00"    # Total block hours
    dom_cycles: int = 0           # Domestic cycles
    int_cycles: int = 0           # International cycles
    total_cycles: int = 0         # Total cycles
    avg_util: str = ""            # Average utilization
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AircraftUtilization':
        """Create from dictionary"""
        return cls(
            date=data.get('date', ''),
            ac_type=data.get('ac_type', ''),
            dom_block=data.get('dom_block', '00:00'),
            int_block=data.get('int_block', '00:00'),
            total_block=data.get('total_block', '00:00'),
            dom_cycles=int(data.get('dom_cycles', 0) or 0),
            int_cycles=int(data.get('int_cycles', 0) or 0),
            total_cycles=int(data.get('total_cycles', 0) or 0),
            avg_util=data.get('avg_util', '')
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'date': self.date,
            'ac_type': self.ac_type,
            'dom_block': self.dom_block,
            'int_block': self.int_block,
            'total_block': self.total_block,
            'dom_cycles': self.dom_cycles,
            'int_cycles': self.int_cycles,
            'total_cycles': self.total_cycles,
            'avg_util': self.avg_util
        }
