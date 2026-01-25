"""
Custom Application Exceptions

Defines structured exception hierarchy for consistent error handling.
"""

from typing import Any, Optional


class AppError(Exception):
    """Base application exception"""
    
    def __init__(
        self, 
        message: str, 
        code: str = "APP_ERROR",
        details: Any = None
    ):
        super().__init__(message)
        self.message = message
        self.code = code
        self.details = details
    
    def to_dict(self) -> dict:
        """Convert exception to dictionary for API responses"""
        return {
            'error': True,
            'code': self.code,
            'message': self.message,
            'details': self.details
        }


class ValidationError(AppError):
    """Input validation failed"""
    
    def __init__(self, message: str, field: Optional[str] = None, details: Any = None):
        super().__init__(
            message=message,
            code="VALIDATION_ERROR",
            details={'field': field, **({'info': details} if details else {})}
        )
        self.field = field


class NotFoundError(AppError):
    """Resource not found"""
    
    def __init__(self, resource: str, identifier: Any = None):
        super().__init__(
            message=f"{resource} not found",
            code="NOT_FOUND",
            details={'resource': resource, 'identifier': identifier}
        )


class DatabaseError(AppError):
    """Database operation failed"""
    
    def __init__(self, operation: str, details: Any = None):
        super().__init__(
            message=f"Database {operation} failed",
            code="DATABASE_ERROR",
            details=details
        )


class ServiceUnavailableError(AppError):
    """External service unavailable"""
    
    def __init__(self, service: str, reason: Optional[str] = None):
        super().__init__(
            message=f"{service} is currently unavailable",
            code="SERVICE_UNAVAILABLE",
            details={'service': service, 'reason': reason}
        )


class AimsConnectionError(ServiceUnavailableError):
    """AIMS API connection failed"""
    
    def __init__(self, reason: Optional[str] = None):
        super().__init__(service="AIMS API", reason=reason)
        self.code = "AIMS_CONNECTION_ERROR"


class CSVParseError(ValidationError):
    """CSV file parsing failed"""
    
    def __init__(self, filename: str, line: Optional[int] = None, reason: Optional[str] = None):
        message = f"Failed to parse CSV file: {filename}"
        if line:
            message += f" at line {line}"
        if reason:
            message += f" - {reason}"
        
        super().__init__(
            message=message,
            field="file",
            details={'filename': filename, 'line': line, 'reason': reason}
        )
        self.code = "CSV_PARSE_ERROR"


class ConfigurationError(AppError):
    """Application configuration error"""
    
    def __init__(self, setting: str, reason: Optional[str] = None):
        super().__init__(
            message=f"Configuration error: {setting}",
            code="CONFIG_ERROR",
            details={'setting': setting, 'reason': reason}
        )


class RateLimitError(AppError):
    """Rate limit exceeded"""
    
    def __init__(self, limit: int, window: str):
        super().__init__(
            message=f"Rate limit exceeded: {limit} requests per {window}",
            code="RATE_LIMIT_EXCEEDED",
            details={'limit': limit, 'window': window}
        )
