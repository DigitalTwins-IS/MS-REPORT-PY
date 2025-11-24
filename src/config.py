"""
Configuración del microservicio MS-REPORT-PY
"""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Configuración de la aplicación"""
    
    # API Configuration
    APP_NAME: str = "MS-REPORT-PY - Reports & Analytics Service"
    APP_VERSION: str = "1.0.0"
    API_PREFIX: str = "/api/v1/reports"
    DEBUG: bool = False
    
    # JWT Configuration (para validar tokens)
    SECRET_KEY: str = "your-secret-key-here-change-in-production"
    ALGORITHM: str = "HS256"
    
    # CORS Configuration
    CORS_ORIGINS: list = [
        "http://localhost:3000",
        "http://localhost:8080",
        "http://localhost"
    ]
    
    # Service Configuration
    SERVICE_HOST: str = "0.0.0.0"
    SERVICE_PORT: int = 8000
    
    # External Services (URLs internas de Docker)
    MS_AUTH_URL: str = "http://ms-auth-py:8000"
    MS_GEO_URL: str = "http://ms-geo-py:8000"
    MS_USER_URL: str = "http://ms-user-py:8000"
    MS_PRODUCT_URL: str = "http://ms-product-py:8000"
    
    # Report Configuration
    MAX_EXPORT_RECORDS: int = 10000
    REPORT_CACHE_TTL: int = 300  # 5 minutos
    
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()

