"""
Date Parsing and Manipulation Utilities

Provides consistent date handling across the application.
"""

from typing import Optional, Tuple
from datetime import datetime, date, timedelta
import re


def parse_date(date_str: str) -> Optional[date]:
    """
    Parse date string in various formats
    
    Supported formats:
    - DD/MM/YY
    - DD/MM/YYYY
    - YYYY-MM-DD
    - DD-MM-YYYY
    - DD.MM.YYYY
    
    Args:
        date_str: Date string to parse
        
    Returns:
        Parsed date or None if invalid
    """
    if not date_str:
        return None
    
    date_str = str(date_str).strip()
    
    formats = [
        '%d/%m/%y',
        '%d/%m/%Y',
        '%Y-%m-%d',
        '%d-%m-%Y',
        '%d.%m.%Y',
        '%Y%m%d',
        '%d%b%y',      # 15Jan26
        '%d%B%Y',      # 15January2026
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    
    return None


def normalize_date(date_str: str, output_format: str = '%d/%m/%y') -> str:
    """
    Normalize date string to consistent format
    
    Args:
        date_str: Input date string
        output_format: Desired output format (default: DD/MM/YY)
        
    Returns:
        Normalized date string or original if parsing fails
    """
    parsed = parse_date(date_str)
    if parsed:
        return parsed.strftime(output_format)
    return date_str


def get_operating_date(calendar_date: str, time_str: str) -> str:
    """
    Determine operating date based on flight departure time.
    
    Operating day: 04:00 to 03:59 next day
    - Flights departing 04:00-23:59: belong to that calendar date
    - Flights departing 00:00-03:59: belong to previous calendar date
    
    Args:
        calendar_date: Calendar date string
        time_str: Departure time (HH:MM)
        
    Returns:
        Operating date string in same format as input
    """
    try:
        # Parse time
        time_minutes = parse_time_to_minutes(time_str)
        if time_minutes is None:
            return calendar_date
        
        # Parse date
        parsed_date = parse_date(calendar_date)
        if not parsed_date:
            return calendar_date
        
        # If departure is before 04:00, operating date is previous day
        if time_minutes < 4 * 60:  # Before 04:00
            operating = parsed_date - timedelta(days=1)
        else:
            operating = parsed_date
        
        # Return in same format as input
        if '/' in calendar_date:
            if len(calendar_date.split('/')[-1]) == 4:
                return operating.strftime('%d/%m/%Y')
            return operating.strftime('%d/%m/%y')
        elif '-' in calendar_date:
            return operating.strftime('%Y-%m-%d')
        
        return operating.strftime('%d/%m/%y')
        
    except Exception:
        return calendar_date


def parse_time_to_minutes(time_str: str) -> Optional[int]:
    """
    Parse time string to minutes from midnight
    
    Supports:
    - "HH:MM"
    - "HHMM"
    - "H:MM"
    
    Args:
        time_str: Time string
        
    Returns:
        Minutes from midnight or None if invalid
    """
    if not time_str:
        return None
    
    time_str = str(time_str).strip()
    
    # Handle HH:MM format
    if ':' in time_str:
        parts = time_str.split(':')
        if len(parts) >= 2:
            try:
                hours = int(parts[0])
                minutes = int(parts[1])
                return hours * 60 + minutes
            except ValueError:
                return None
    
    # Handle HHMM format
    if len(time_str) == 4 and time_str.isdigit():
        try:
            hours = int(time_str[:2])
            minutes = int(time_str[2:])
            return hours * 60 + minutes
        except ValueError:
            return None
    
    return None


def minutes_to_time(minutes: int) -> str:
    """
    Convert minutes to HH:MM format
    
    Args:
        minutes: Minutes from midnight
        
    Returns:
        Time string in HH:MM format
    """
    if minutes is None or minutes < 0:
        return "00:00"
    
    hours = (minutes // 60) % 24
    mins = minutes % 60
    return f"{hours:02d}:{mins:02d}"


def parse_hours_string(time_str: str) -> float:
    """
    Parse hours string (HH:MM) to decimal hours
    
    Examples:
    - "85:30" -> 85.5
    - "2:15" -> 2.25
    
    Args:
        time_str: Time string in HH:MM format
        
    Returns:
        Decimal hours
    """
    if not time_str:
        return 0.0
    
    time_str = str(time_str).strip()
    
    if ':' in time_str:
        parts = time_str.split(':')
        try:
            hours = int(parts[0])
            minutes = int(parts[1]) if len(parts) > 1 else 0
            return hours + minutes / 60
        except ValueError:
            return 0.0
    
    # Try parsing as decimal
    try:
        return float(time_str)
    except ValueError:
        return 0.0


def get_date_range(
    target_date: Optional[date] = None,
    days_back: int = 30,
    days_forward: int = 30
) -> Tuple[date, date]:
    """
    Get date range around target date
    
    Args:
        target_date: Center date (default: today)
        days_back: Days before target
        days_forward: Days after target
        
    Returns:
        Tuple of (from_date, to_date)
    """
    if not target_date:
        target_date = date.today()
    
    from_date = target_date - timedelta(days=days_back)
    to_date = target_date + timedelta(days=days_forward)
    
    return from_date, to_date


def format_date_for_display(d: date, include_day: bool = True) -> str:
    """Format date for dashboard display"""
    if include_day:
        return d.strftime('%a, %d %b %Y')  # Mon, 15 Jan 2026
    return d.strftime('%d %b %Y')  # 15 Jan 2026
