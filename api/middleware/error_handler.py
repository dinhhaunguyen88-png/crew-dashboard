"""
Centralized Error Handling Middleware for Flask

Provides consistent error responses and logging across all endpoints.
"""

from flask import Flask, jsonify, request
from functools import wraps
import logging
import traceback
from typing import Tuple, Dict, Any, Callable

from app.errors import (
    AppError, 
    ValidationError, 
    NotFoundError, 
    DatabaseError,
    ServiceUnavailableError,
    AimsConnectionError,
    CSVParseError
)

logger = logging.getLogger(__name__)


def setup_error_handlers(app: Flask):
    """
    Register error handlers with Flask app
    
    Usage:
        from api.middleware.error_handler import setup_error_handlers
        
        app = Flask(__name__)
        setup_error_handlers(app)
    """
    
    @app.errorhandler(AppError)
    def handle_app_error(error: AppError) -> Tuple[Dict, int]:
        """Handle custom application errors"""
        logger.warning(f"App Error [{error.code}]: {error.message}")
        
        status_code = _get_status_code(error)
        return jsonify(error.to_dict()), status_code
    
    @app.errorhandler(ValidationError)
    def handle_validation_error(error: ValidationError) -> Tuple[Dict, int]:
        """Handle validation errors"""
        logger.warning(f"Validation Error: {error.message} (field: {error.field})")
        return jsonify(error.to_dict()), 400
    
    @app.errorhandler(NotFoundError)
    def handle_not_found_error(error: NotFoundError) -> Tuple[Dict, int]:
        """Handle not found errors"""
        logger.info(f"Not Found: {error.message}")
        return jsonify(error.to_dict()), 404
    
    @app.errorhandler(DatabaseError)
    def handle_database_error(error: DatabaseError) -> Tuple[Dict, int]:
        """Handle database errors"""
        logger.error(f"Database Error: {error.message}")
        return jsonify(error.to_dict()), 500
    
    @app.errorhandler(ServiceUnavailableError)
    def handle_service_unavailable(error: ServiceUnavailableError) -> Tuple[Dict, int]:
        """Handle service unavailable errors"""
        logger.error(f"Service Unavailable: {error.message}")
        return jsonify(error.to_dict()), 503
    
    @app.errorhandler(404)
    def handle_flask_not_found(error) -> Tuple[Dict, int]:
        """Handle Flask 404 errors"""
        return jsonify({
            'error': True,
            'code': 'ENDPOINT_NOT_FOUND',
            'message': f"Endpoint not found: {request.path}",
            'details': {'method': request.method, 'path': request.path}
        }), 404
    
    @app.errorhandler(405)
    def handle_method_not_allowed(error) -> Tuple[Dict, int]:
        """Handle method not allowed errors"""
        return jsonify({
            'error': True,
            'code': 'METHOD_NOT_ALLOWED',
            'message': f"Method {request.method} not allowed for {request.path}",
            'details': None
        }), 405
    
    @app.errorhandler(413)
    def handle_payload_too_large(error) -> Tuple[Dict, int]:
        """Handle payload too large errors"""
        return jsonify({
            'error': True,
            'code': 'PAYLOAD_TOO_LARGE',
            'message': "File too large. Maximum size is 16MB.",
            'details': None
        }), 413
    
    @app.errorhandler(500)
    def handle_server_error(error) -> Tuple[Dict, int]:
        """Handle internal server errors"""
        logger.error(f"Server Error: {error}")
        logger.error(traceback.format_exc())
        return jsonify({
            'error': True,
            'code': 'INTERNAL_ERROR',
            'message': 'An internal error occurred',
            'details': None
        }), 500
    
    @app.errorhandler(Exception)
    def handle_unexpected_error(error) -> Tuple[Dict, int]:
        """Handle all unexpected exceptions"""
        logger.error(f"Unexpected Error: {type(error).__name__}: {error}")
        logger.error(traceback.format_exc())
        return jsonify({
            'error': True,
            'code': 'UNEXPECTED_ERROR',
            'message': 'An unexpected error occurred',
            'details': {'type': type(error).__name__} if app.debug else None
        }), 500


def _get_status_code(error: AppError) -> int:
    """Map error codes to HTTP status codes"""
    code_map = {
        'VALIDATION_ERROR': 400,
        'CSV_PARSE_ERROR': 400,
        'NOT_FOUND': 404,
        'DATABASE_ERROR': 500,
        'SERVICE_UNAVAILABLE': 503,
        'AIMS_CONNECTION_ERROR': 503,
        'CONFIG_ERROR': 500,
        'RATE_LIMIT_EXCEEDED': 429,
    }
    return code_map.get(error.code, 500)


def safe_endpoint(func: Callable) -> Callable:
    """
    Decorator for safe endpoint execution with error handling
    
    Catches exceptions and converts them to proper API responses.
    
    Usage:
        @app.route('/api/data')
        @safe_endpoint
        def get_data():
            # Your code here
            pass
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except AppError:
            # Let the error handler deal with it
            raise
        except Exception as e:
            logger.error(f"Endpoint {func.__name__} failed: {e}")
            logger.error(traceback.format_exc())
            # Convert to AppError for consistent handling
            raise AppError(
                message=f"Operation failed: {str(e)}",
                code="ENDPOINT_ERROR"
            )
    return wrapper


def log_request():
    """Log incoming request details"""
    logger.debug(f"Request: {request.method} {request.path}")
    if request.content_length:
        logger.debug(f"Content-Length: {request.content_length}")


def log_response(response):
    """Log response details"""
    logger.debug(f"Response: {response.status_code}")
    return response


def setup_request_logging(app: Flask):
    """
    Setup request/response logging
    
    Usage:
        setup_request_logging(app)
    """
    app.before_request(log_request)
    app.after_request(log_response)
