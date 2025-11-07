"""
Schemas para Reportes y Análisis
"""
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime


class CoverageByZoneItem(BaseModel):
    """Item de cobertura por zona"""
    zone_id: int = Field(..., description="ID de la zona")
    zone_name: str = Field(..., description="Nombre de la zona")
    city_name: str = Field(..., description="Nombre de la ciudad")
    total_sellers: int = Field(..., description="Total de vendedores")
    total_shopkeepers: int = Field(..., description="Total de tenderos")
    coverage_percentage: float = Field(..., description="Porcentaje de cobertura")
    
    class Config:
        json_schema_extra = {
            "example": {
                "zone_id": 1,
                "zone_name": "Norte",
                "city_name": "Bogotá",
                "total_sellers": 3,
                "total_shopkeepers": 45,
                "coverage_percentage": 75.0
            }
        }


class CoverageReportResponse(BaseModel):
    """Reporte de cobertura territorial"""
    report_date: datetime = Field(..., description="Fecha del reporte")
    total_cities: int = Field(..., description="Total de ciudades")
    total_zones: int = Field(..., description="Total de zonas")
    total_sellers: int = Field(..., description="Total de vendedores")
    total_shopkeepers: int = Field(..., description="Total de tenderos")
    zones: List[CoverageByZoneItem] = Field(..., description="Detalle por zona")
    
    class Config:
        json_schema_extra = {
            "example": {
                "report_date": "2025-10-02T00:00:00Z",
                "total_cities": 3,
                "total_zones": 9,
                "total_sellers": 25,
                "total_shopkeepers": 320,
                "zones": []
            }
        }


class SellerPerformanceItem(BaseModel):
    """Rendimiento individual de vendedor"""
    seller_id: int = Field(..., description="ID del vendedor")
    seller_name: str = Field(..., description="Nombre del vendedor")
    seller_email: str = Field(..., description="Email del vendedor")
    zone_name: str = Field(..., description="Zona asignada")
    total_shopkeepers: int = Field(..., description="Total de tenderos asignados")
    is_over_limit: bool = Field(..., description="Supera el límite recomendado")
    efficiency_score: float = Field(..., description="Score de eficiencia (0-100)")
    
    class Config:
        json_schema_extra = {
            "example": {
                "seller_id": 1,
                "seller_name": "Juan Pérez",
                "seller_email": "juan@vendedor.com",
                "zone_name": "Norte",
                "total_shopkeepers": 45,
                "is_over_limit": False,
                "efficiency_score": 85.5
            }
        }


class SellerPerformanceReportResponse(BaseModel):
    """Reporte de rendimiento de vendedores"""
    report_date: datetime = Field(..., description="Fecha del reporte")
    total_sellers: int = Field(..., description="Total de vendedores")
    avg_shopkeepers_per_seller: float = Field(..., description="Promedio de tenderos por vendedor")
    sellers: List[SellerPerformanceItem] = Field(..., description="Detalle por vendedor")
    
    class Config:
        json_schema_extra = {
            "example": {
                "report_date": "2025-10-02T00:00:00Z",
                "total_sellers": 25,
                "avg_shopkeepers_per_seller": 32.5,
                "sellers": []
            }
        }


class ZoneStatistics(BaseModel):
    """Estadísticas de una zona"""
    zone_id: int = Field(..., description="ID de la zona")
    zone_name: str = Field(..., description="Nombre de la zona")
    city_name: str = Field(..., description="Ciudad")
    sellers_count: int = Field(..., description="Cantidad de vendedores")
    shopkeepers_count: int = Field(..., description="Cantidad de tenderos")
    assigned_shopkeepers: int = Field(..., description="Tenderos asignados")
    unassigned_shopkeepers: int = Field(..., description="Tenderos sin asignar")
    avg_shopkeepers_per_seller: float = Field(..., description="Promedio por vendedor")
    
    class Config:
        json_schema_extra = {
            "example": {
                "zone_id": 1,
                "zone_name": "Norte",
                "city_name": "Bogotá",
                "sellers_count": 3,
                "shopkeepers_count": 50,
                "assigned_shopkeepers": 45,
                "unassigned_shopkeepers": 5,
                "avg_shopkeepers_per_seller": 15.0
            }
        }


class SystemMetrics(BaseModel):
    """Métricas generales del sistema"""
    total_cities: int = Field(..., description="Total de ciudades")
    total_zones: int = Field(..., description="Total de zonas")
    total_sellers: int = Field(..., description="Total de vendedores")
    total_shopkeepers: int = Field(..., description="Total de tenderos")
    active_assignments: int = Field(..., description="Asignaciones activas")
    unassigned_shopkeepers: int = Field(..., description="Tenderos sin vendedor")
    avg_shopkeepers_per_seller: float = Field(..., description="Promedio tenderos/vendedor")
    system_health: str = Field(..., description="Estado del sistema")
    
    class Config:
        json_schema_extra = {
            "example": {
                "total_cities": 3,
                "total_zones": 9,
                "total_sellers": 25,
                "total_shopkeepers": 320,
                "active_assignments": 300,
                "unassigned_shopkeepers": 20,
                "avg_shopkeepers_per_seller": 12.8,
                "system_health": "healthy"
            }
        }


class ExportRequest(BaseModel):
    """Solicitud de exportación"""
    report_type: str = Field(..., description="Tipo de reporte (coverage, sellers, zones)")
    format: str = Field("csv", description="Formato (csv, excel, json)")
    city_id: Optional[int] = Field(None, description="Filtrar por ciudad")
    zone_id: Optional[int] = Field(None, description="Filtrar por zona")
    
    class Config:
        json_schema_extra = {
            "example": {
                "report_type": "coverage",
                "format": "csv",
                "city_id": 1
            }
        }


class SalesComparisonItem(BaseModel):
    """Item de comparación de ventas por zona/ciudad"""
    zone_id: int = Field(..., description="ID de la zona")
    zone_name: str = Field(..., description="Nombre de la zona")
    city_id: int = Field(..., description="ID de la ciudad")
    city_name: str = Field(..., description="Nombre de la ciudad")
    total_shopkeepers: int = Field(..., description="Total de tenderos asignados")
    total_sellers: int = Field(..., description="Total de vendedores")
    performance_score: float = Field(..., description="Score de desempeño (0-100)")
    market_penetration: float = Field(..., description="Penetración de mercado (tenderos promedio)")
    
    class Config:
        json_schema_extra = {
            "example": {
                "zone_id": 1,
                "zone_name": "Norte",
                "city_id": 1,
                "city_name": "Bogotá",
                "total_shopkeepers": 145,
                "total_sellers": 5,
                "performance_score": 85.5,
                "market_penetration": 29.0
            }
        }


class CitySalesComparisonItem(BaseModel):
    """Item de comparación de ventas por ciudad"""
    city_id: int = Field(..., description="ID de la ciudad")
    city_name: str = Field(..., description="Nombre de la ciudad")
    total_zones: int = Field(..., description="Total de zonas en la ciudad")
    total_shopkeepers: int = Field(..., description="Total de tenderos asignados")
    total_sellers: int = Field(..., description="Total de vendedores")
    performance_score: float = Field(..., description="Score de desempeño (0-100)")
    market_penetration: float = Field(..., description="Penetración de mercado (tenderos promedio)")
    
    class Config:
        json_schema_extra = {
            "example": {
                "city_id": 1,
                "city_name": "Bogotá",
                "total_zones": 3,
                "total_shopkeepers": 320,
                "total_sellers": 12,
                "performance_score": 88.5,
                "market_penetration": 26.67
            }
        }


class SalesComparisonResponse(BaseModel):
    """Respuesta de comparación de ventas por zonas y ciudades"""
    report_date: datetime = Field(..., description="Fecha del reporte")
    comparison_type: str = Field(..., description="Tipo de comparación (zones, cities, both)")
    zones: List[SalesComparisonItem] = Field(default_factory=list, description="Comparación por zona")
    cities: List[CitySalesComparisonItem] = Field(default_factory=list, description="Comparación por ciudad")
    top_zones: List[SalesComparisonItem] = Field(default_factory=list, description="Zonas con mejor desempeño")
    top_cities: List[CitySalesComparisonItem] = Field(default_factory=list, description="Ciudades con mejor desempeño")
    
    class Config:
        json_schema_extra = {
            "example": {
                "report_date": "2025-10-02T00:00:00Z",
                "comparison_type": "both",
                "zones": [],
                "cities": [],
                "top_zones": [],
                "top_cities": []
            }
        }


class HealthResponse(BaseModel):
    """Health check response"""
    status: str = Field(..., description="Estado del servicio")
    service: str = Field(..., description="Nombre del servicio")
    version: str = Field(..., description="Versión")
    external_services: dict = Field(..., description="Estado de servicios externos")
    
    class Config:
        json_schema_extra = {
            "example": {
                "status": "healthy",
                "service": "MS-REPORT-PY",
                "version": "1.0.0",
                "external_services": {
                    "ms-auth": "connected",
                    "ms-geo": "connected",
                    "ms-user": "connected"
                }
            }
        }

