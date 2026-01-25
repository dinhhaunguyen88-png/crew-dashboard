"""
Base Service Interface

Defines abstract interface for data services.
Both CSV and AIMS services implement this interface.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Generic, TypeVar
from datetime import datetime, date
from dataclasses import dataclass


T = TypeVar('T')


@dataclass
class ServiceResult(Generic[T]):
    """Standard service response wrapper"""
    success: bool
    data: Optional[T] = None
    error: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    
    @classmethod
    def ok(cls, data: T, metadata: Dict[str, Any] = None) -> 'ServiceResult[T]':
        """Create successful result"""
        return cls(success=True, data=data, metadata=metadata)
    
    @classmethod
    def fail(cls, error: str, metadata: Dict[str, Any] = None) -> 'ServiceResult[T]':
        """Create failure result"""
        return cls(success=False, error=error, metadata=metadata)


class IDataService(ABC):
    """
    Abstract interface for data services
    
    Implemented by:
    - CSVService: Loads data from CSV files
    - AimsService: Fetches data from AIMS SOAP API
    - SupabaseService: Retrieves data from Supabase
    """
    
    @abstractmethod
    def get_flights(
        self, 
        from_date: Optional[date] = None,
        to_date: Optional[date] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """
        Get flight data for date range
        
        Args:
            from_date: Start date (inclusive)
            to_date: End date (inclusive)
            
        Returns:
            ServiceResult containing list of flight dictionaries
        """
        pass
    
    @abstractmethod
    def get_crew_schedule(
        self, 
        target_date: Optional[date] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Get crew schedule data
        
        Args:
            target_date: Specific date to filter
            
        Returns:
            ServiceResult containing crew schedule with:
            - records: List of individual schedule records
            - summary: Counts by status type
        """
        pass
    
    @abstractmethod
    def get_rolling_hours(
        self, 
        crew_id: Optional[str] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """
        Get rolling block hours data
        
        Args:
            crew_id: Optional filter for specific crew member
            
        Returns:
            ServiceResult containing list of rolling hour records
        """
        pass
    
    @abstractmethod
    def get_utilization(
        self, 
        target_date: Optional[date] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Get aircraft utilization data
        
        Args:
            target_date: Specific date to filter
            
        Returns:
            ServiceResult containing utilization metrics by AC type
        """
        pass
    
    @abstractmethod
    def is_available(self) -> bool:
        """Check if service is available and configured"""
        pass
    
    @abstractmethod
    def test_connection(self) -> ServiceResult[Dict[str, Any]]:
        """Test connection to data source"""
        pass


class ICrewService(ABC):
    """
    Crew-specific service interface
    
    Extended interface for crew-related operations.
    """
    
    @abstractmethod
    def get_crew_member(self, crew_id: str) -> ServiceResult[Dict[str, Any]]:
        """Get crew member details by ID"""
        pass
    
    @abstractmethod
    def get_crew_list(
        self,
        base: Optional[str] = None,
        ac_type: Optional[str] = None,
        position: Optional[str] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Get list of crew members with optional filters"""
        pass
    
    @abstractmethod
    def calculate_rolling_hours(
        self,
        crew_id: str,
        as_of_date: Optional[date] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Calculate rolling 28-day/365-day hours for crew member"""
        pass


class IFlightService(ABC):
    """
    Flight-specific service interface
    """
    
    @abstractmethod
    def get_flight_details(
        self,
        flight_id: str
    ) -> ServiceResult[Dict[str, Any]]:
        """Get detailed flight information"""
        pass
    
    @abstractmethod
    def get_leg_members(
        self,
        leg_date: date
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Get crew assignments per flight leg"""
        pass
