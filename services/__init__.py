"""
Services Package - Business Logic Layer
"""

from services.base_service import IDataService, ServiceResult
from services.aims_service import AimsService, get_aims_service

__all__ = [
    'IDataService',
    'ServiceResult',
    'AimsService',
    'get_aims_service'
]
