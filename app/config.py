"""
Application Configuration

Centralized configuration management with validation.
Loads settings from environment variables with sensible defaults.
"""

import os
from dataclasses import dataclass
from typing import Optional
import logging

logger = logging.getLogger(__name__)


@dataclass
class SupabaseConfig:
    """Supabase database configuration"""
    url: str
    key: str
    
    @classmethod
    def from_env(cls) -> Optional['SupabaseConfig']:
        """Load Supabase config from environment"""
        url = os.environ.get('SUPABASE_URL')
        key = os.environ.get('SUPABASE_KEY')
        
        if not url or not key:
            logger.warning("SUPABASE_URL or SUPABASE_KEY not set")
            return None
        
        return cls(url=url, key=key)
    
    def is_valid(self) -> bool:
        """Check if config is valid"""
        return bool(self.url and self.key)


@dataclass  
class AimsConfig:
    """AIMS SOAP API configuration"""
    enabled: bool
    wsdl_url: Optional[str]
    username: Optional[str]
    password: Optional[str]
    timeout: int = 30
    max_retries: int = 3
    
    @classmethod
    def from_env(cls) -> 'AimsConfig':
        """Load AIMS config from environment"""
        return cls(
            enabled=os.environ.get('AIMS_ENABLED', 'false').lower() == 'true',
            wsdl_url=os.environ.get('AIMS_WSDL_URL'),
            username=os.environ.get('AIMS_USERNAME'),
            password=os.environ.get('AIMS_PASSWORD'),
            timeout=int(os.environ.get('AIMS_TIMEOUT', '30')),
            max_retries=int(os.environ.get('AIMS_MAX_RETRIES', '3'))
        )
    
    def is_ready(self) -> bool:
        """Check if AIMS is properly configured and enabled"""
        return (
            self.enabled and 
            bool(self.wsdl_url) and 
            bool(self.username) and 
            bool(self.password)
        )


@dataclass
class FeatureFlags:
    """Feature toggle flags"""
    file_watcher: bool = True
    auto_refresh: bool = True
    aims_integration: bool = False
    
    @classmethod
    def from_env(cls) -> 'FeatureFlags':
        """Load feature flags from environment"""
        return cls(
            file_watcher=os.environ.get('FEATURE_FILE_WATCHER', 'true').lower() == 'true',
            auto_refresh=os.environ.get('FEATURE_AUTO_REFRESH', 'true').lower() == 'true',
            aims_integration=os.environ.get('FEATURE_AIMS_INTEGRATION', 'false').lower() == 'true'
        )


@dataclass
class AppConfig:
    """Main application configuration"""
    debug: bool
    secret_key: str
    log_level: str
    supabase: Optional[SupabaseConfig]
    aims: AimsConfig
    features: FeatureFlags
    
    @classmethod
    def from_env(cls) -> 'AppConfig':
        """Load all configuration from environment"""
        return cls(
            debug=os.environ.get('DEBUG', 'false').lower() == 'true',
            secret_key=os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production'),
            log_level=os.environ.get('LOG_LEVEL', 'INFO'),
            supabase=SupabaseConfig.from_env(),
            aims=AimsConfig.from_env(),
            features=FeatureFlags.from_env()
        )
    
    def validate(self) -> list:
        """Validate configuration and return list of issues"""
        issues = []
        
        if self.secret_key == 'dev-secret-key-change-in-production':
            issues.append("SECRET_KEY should be changed in production")
        
        if not self.supabase:
            issues.append("Supabase not configured - running in local-only mode")
        
        if self.features.aims_integration and not self.aims.is_ready():
            issues.append("AIMS integration enabled but not properly configured")
        
        return issues


# Singleton config instance
_config: Optional[AppConfig] = None


def get_config() -> AppConfig:
    """Get or create application config singleton"""
    global _config
    if _config is None:
        _config = AppConfig.from_env()
        
        # Log configuration status
        issues = _config.validate()
        for issue in issues:
            logger.warning(f"Config: {issue}")
        
        logger.info(f"Config loaded - Debug: {_config.debug}, AIMS: {_config.aims.is_ready()}")
    
    return _config


def reload_config() -> AppConfig:
    """Force reload configuration from environment"""
    global _config
    _config = None
    return get_config()
