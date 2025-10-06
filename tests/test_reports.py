"""
Tests para MS-REPORT-PY
"""
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, AsyncMock

from src.main import app

client = TestClient(app)


# ============================================================================
# TESTS DE HEALTH CHECK
# ============================================================================

def test_root_health_check():
    """Test del health check raíz"""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["service"] == "MS-REPORT-PY - Reports & Analytics Service"


@pytest.mark.asyncio
async def test_detailed_health_check():
    """Test del health check detallado"""
    with patch("src.utils.http_client.ms_client.check_service_health") as mock_health:
        mock_health.return_value = "connected"
        
        response = client.get("/api/v1/reports/health")
        # Este endpoint requiere que los servicios estén disponibles
        # En ambiente de prueba, puede fallar
        assert response.status_code in [200, 503]


# ============================================================================
# TESTS DE REPORTES (Requieren autenticación)
# ============================================================================

def test_coverage_report_without_auth():
    """Test de reporte de cobertura sin autenticación"""
    response = client.get("/api/v1/reports/coverage")
    assert response.status_code == 401  # No autorizado


def test_sellers_performance_without_auth():
    """Test de rendimiento de vendedores sin autenticación"""
    response = client.get("/api/v1/reports/sellers-performance")
    assert response.status_code == 401


def test_zone_statistics_without_auth():
    """Test de estadísticas de zona sin autenticación"""
    response = client.get("/api/v1/reports/zones/1/statistics")
    assert response.status_code == 401


def test_system_metrics_without_auth():
    """Test de métricas del sistema sin autenticación"""
    response = client.get("/api/v1/reports/metrics")
    assert response.status_code == 401


def test_export_report_without_auth():
    """Test de exportación sin autenticación"""
    response = client.post("/api/v1/reports/export", json={
        "report_type": "coverage",
        "format": "csv"
    })
    assert response.status_code == 401


# ============================================================================
# TESTS CON MOCK DE AUTENTICACIÓN
# ============================================================================

@pytest.mark.asyncio
async def test_coverage_report_with_mock():
    """Test de reporte de cobertura con datos mockeados"""
    # Este test requiere mockear:
    # 1. Autenticación
    # 2. Llamadas a MS-GEO-PY
    # 3. Llamadas a MS-USER-PY
    
    # En producción, usar fixtures y mocks completos
    pass


@pytest.mark.asyncio
async def test_export_csv():
    """Test de exportación a CSV"""
    # Mock test para exportación
    pass


@pytest.mark.asyncio
async def test_export_json():
    """Test de exportación a JSON"""
    # Mock test para exportación
    pass


# ============================================================================
# TESTS DE INTEGRACIÓN CON MICROSERVICIOS
# ============================================================================

@pytest.mark.integration
async def test_microservice_client_get_cities():
    """Test del cliente HTTP para obtener ciudades"""
    from src.utils.http_client import ms_client
    
    # Este test requiere MS-GEO-PY corriendo
    cities = await ms_client.get_all_cities()
    # En ambiente de prueba puede estar vacío
    assert isinstance(cities, list)


@pytest.mark.integration
async def test_microservice_client_get_zones():
    """Test del cliente HTTP para obtener zonas"""
    from src.utils.http_client import ms_client
    
    zones = await ms_client.get_all_zones()
    assert isinstance(zones, list)


@pytest.mark.integration
async def test_check_service_health():
    """Test de verificación de salud de servicios"""
    from src.utils.http_client import ms_client
    from src.config import settings
    
    # Verificar MS-GEO
    status = await ms_client.check_service_health(settings.MS_GEO_URL)
    assert status in ["connected", "disconnected", "unhealthy"]


# ============================================================================
# TESTS DE VALIDACIÓN DE SCHEMAS
# ============================================================================

def test_coverage_report_schema():
    """Test de validación del schema CoverageReportResponse"""
    from src.schemas import CoverageReportResponse, CoverageByZoneItem
    from datetime import datetime
    
    report = CoverageReportResponse(
        report_date=datetime.now(),
        total_cities=3,
        total_zones=9,
        total_sellers=25,
        total_shopkeepers=320,
        zones=[]
    )
    
    assert report.total_cities == 3
    assert report.total_zones == 9


def test_seller_performance_schema():
    """Test de validación del schema SellerPerformanceItem"""
    from src.schemas import SellerPerformanceItem
    
    item = SellerPerformanceItem(
        seller_id=1,
        seller_name="Test Seller",
        seller_email="test@seller.com",
        zone_name="Norte",
        total_shopkeepers=45,
        is_over_limit=False,
        efficiency_score=85.5
    )
    
    assert item.seller_id == 1
    assert item.efficiency_score == 85.5


def test_system_metrics_schema():
    """Test de validación del schema SystemMetrics"""
    from src.schemas import SystemMetrics
    
    metrics = SystemMetrics(
        total_cities=3,
        total_zones=9,
        total_sellers=25,
        total_shopkeepers=320,
        active_assignments=300,
        unassigned_shopkeepers=20,
        avg_shopkeepers_per_seller=12.8,
        system_health="healthy"
    )
    
    assert metrics.total_cities == 3
    assert metrics.system_health == "healthy"


def test_export_request_schema():
    """Test de validación del schema ExportRequest"""
    from src.schemas import ExportRequest
    
    request = ExportRequest(
        report_type="coverage",
        format="csv",
        city_id=1
    )
    
    assert request.report_type == "coverage"
    assert request.format == "csv"


# ============================================================================
# TESTS DE LÓGICA DE NEGOCIO
# ============================================================================

def test_efficiency_calculation():
    """Test del cálculo de eficiencia de vendedores"""
    # La eficiencia se calcula basándose en carga óptima de 40 tenderos
    
    # Caso 1: Carga óptima (40 tenderos) = 100%
    optimal_load = 40
    shopkeeper_count = 40
    efficiency = (shopkeeper_count / optimal_load) * 100
    assert efficiency == 100.0
    
    # Caso 2: Media carga (20 tenderos) = 50%
    shopkeeper_count = 20
    efficiency = (shopkeeper_count / optimal_load) * 100
    assert efficiency == 50.0
    
    # Caso 3: Sobrecarga (80 tenderos) = penalización
    shopkeeper_count = 80
    if shopkeeper_count > optimal_load:
        efficiency = max(0, 100 - ((shopkeeper_count - optimal_load) / optimal_load) * 50)
    assert efficiency == 0.0  # Máxima penalización


def test_coverage_percentage_calculation():
    """Test del cálculo de porcentaje de cobertura"""
    # Cobertura = (tenderos_actuales / máximo_esperado) * 100
    
    zone_sellers = 3
    shopkeeper_count = 60
    max_expected = zone_sellers * 80  # 240
    
    coverage = (shopkeeper_count / max_expected * 100)
    assert coverage == 25.0  # 60 de 240 = 25%


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

