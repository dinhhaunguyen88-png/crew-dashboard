"""
Input Validation Utilities

Provides validation functions for CSV files, dates, and other inputs.
"""

from typing import Tuple, Optional, List, Dict, Any
import re
from datetime import date, datetime


class CSVValidator:
    """Validate CSV file contents before processing"""
    
    # Required columns for each CSV type
    REQUIRED_COLUMNS = {
        'dayrep': ['date', 'reg', 'flt'],
        'sacutil': ['date', 'ac'],
        'rolcrtot': ['id', 'name', '28 day'],
        'crew_schedule': []  # More flexible
    }
    
    @classmethod
    def validate_dayrep(cls, content: bytes) -> Tuple[bool, Optional[str]]:
        """
        Validate DayRepReport CSV structure
        
        Args:
            content: Raw file bytes
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            text = cls._decode_content(content)
            if not text:
                return False, "CSV file is empty"
            
            lines = text.strip().split('\n')
            
            if len(lines) < 2:
                return False, "CSV file has no data rows"
            
            # Check for required columns in header
            header = lines[0].lower()
            missing = []
            for col in cls.REQUIRED_COLUMNS['dayrep']:
                if col not in header:
                    missing.append(col)
            
            if missing:
                return False, f"Missing required columns: {', '.join(missing)}"
            
            return True, None
            
        except UnicodeDecodeError:
            return False, "Invalid file encoding. Please use UTF-8."
        except Exception as e:
            return False, f"Validation error: {str(e)}"
    
    @classmethod
    def validate_sacutil(cls, content: bytes) -> Tuple[bool, Optional[str]]:
        """Validate SacutilReport CSV structure"""
        try:
            text = cls._decode_content(content)
            if not text:
                return False, "CSV file is empty"
            
            lines = text.strip().split('\n')
            if len(lines) < 2:
                return False, "CSV file has no data rows"
            
            return True, None
            
        except Exception as e:
            return False, f"Validation error: {str(e)}"
    
    @classmethod
    def validate_rolcrtot(cls, content: bytes) -> Tuple[bool, Optional[str]]:
        """Validate RolCrTotReport CSV structure"""
        try:
            text = cls._decode_content(content)
            if not text:
                return False, "CSV file is empty"
            
            lines = text.strip().split('\n')
            if len(lines) < 2:
                return False, "CSV file has no data rows"
            
            # Check for 28-day column
            header = lines[0].lower()
            if '28' not in header and 'day' not in header:
                return False, "Missing rolling hours columns"
            
            return True, None
            
        except Exception as e:
            return False, f"Validation error: {str(e)}"
    
    @classmethod
    def validate_crew_schedule(cls, content: bytes) -> Tuple[bool, Optional[str]]:
        """Validate Crew Schedule CSV structure"""
        try:
            text = cls._decode_content(content)
            if not text:
                return False, "CSV file is empty"
            
            lines = text.strip().split('\n')
            if len(lines) < 5:  # Need header + period info + data
                return False, "CSV file appears incomplete"
            
            return True, None
            
        except Exception as e:
            return False, f"Validation error: {str(e)}"
    
    @staticmethod
    def _decode_content(content: bytes) -> str:
        """Decode bytes with encoding fallback"""
        for encoding in ['utf-8', 'cp1252', 'latin1']:
            try:
                return content.decode(encoding)
            except UnicodeDecodeError:
                continue
        raise UnicodeDecodeError("Failed to decode with any encoding")
    
    @classmethod
    def validate_file(cls, content: bytes, file_type: str) -> Tuple[bool, Optional[str]]:
        """
        Validate CSV file by type
        
        Args:
            content: Raw file bytes
            file_type: One of 'dayrep', 'sacutil', 'rolcrtot', 'crew_schedule'
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        validators = {
            'dayrep': cls.validate_dayrep,
            'sacutil': cls.validate_sacutil,
            'rolcrtot': cls.validate_rolcrtot,
            'crew_schedule': cls.validate_crew_schedule
        }
        
        validator = validators.get(file_type)
        if not validator:
            return False, f"Unknown file type: {file_type}"
        
        return validator(content)


def validate_date_range(
    from_date: Optional[str],
    to_date: Optional[str]
) -> Tuple[bool, Optional[str], Optional[date], Optional[date]]:
    """
    Validate and parse date range
    
    Args:
        from_date: Start date string
        to_date: End date string
        
    Returns:
        Tuple of (is_valid, error_message, parsed_from, parsed_to)
    """
    parsed_from = None
    parsed_to = None
    
    if from_date:
        parsed_from = parse_date_string(from_date)
        if not parsed_from:
            return False, f"Invalid from_date format: {from_date}", None, None
    
    if to_date:
        parsed_to = parse_date_string(to_date)
        if not parsed_to:
            return False, f"Invalid to_date format: {to_date}", None, None
    
    if parsed_from and parsed_to and parsed_from > parsed_to:
        return False, "from_date must be before to_date", None, None
    
    return True, None, parsed_from, parsed_to


def parse_date_string(date_str: str) -> Optional[date]:
    """
    Parse date string in various formats
    
    Supported formats:
    - DD/MM/YY
    - DD/MM/YYYY
    - YYYY-MM-DD
    - DD-MM-YYYY
    """
    if not date_str:
        return None
    
    date_str = date_str.strip()
    
    formats = [
        '%d/%m/%y',
        '%d/%m/%Y',
        '%Y-%m-%d',
        '%d-%m-%Y',
        '%d.%m.%Y',
        '%Y%m%d'
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    
    return None


def validate_crew_id(crew_id: str) -> Tuple[bool, Optional[str]]:
    """Validate crew ID format"""
    if not crew_id:
        return False, "Crew ID is required"
    
    # Crew IDs are typically numeric
    if not re.match(r'^\d+$', crew_id.strip()):
        return False, "Crew ID must be numeric"
    
    return True, None


def validate_file_extension(filename: str, allowed: List[str]) -> Tuple[bool, Optional[str]]:
    """Validate file extension"""
    if not filename:
        return False, "Filename is required"
    
    ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
    
    if ext not in allowed:
        return False, f"File type not allowed. Allowed: {', '.join(allowed)}"
    
    return True, None
