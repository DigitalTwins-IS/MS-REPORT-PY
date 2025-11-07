"""
Router de Reportes y Análisis
"""
from fastapi import APIRouter, Depends, HTTPException, status, Query, Header
from fastapi.responses import StreamingResponse
from typing import Optional, List
from datetime import datetime
import io
import csv
import json

from ..schemas import (
    CoverageReportResponse,
    CoverageByZoneItem,
    SellerPerformanceReportResponse,
    SellerPerformanceItem,
    ZoneStatistics,
    SystemMetrics,
    ExportRequest,
    HealthResponse,
    SalesComparisonResponse,
    SalesComparisonItem,
    CitySalesComparisonItem
)
from ..utils import get_current_user, ms_client
from ..config import settings

router = APIRouter()


@router.get("/coverage", response_model=CoverageReportResponse)
async def get_coverage_report(
    city_id: Optional[int] = Query(None, description="Filtrar por ciudad"),
    current_user: dict = Depends(get_current_user),
    authorization: str = Header(None)
):
    """
    Reporte de cobertura territorial por zonas
    
    Muestra la distribución de vendedores y tenderos por zona
    """
    token = authorization.replace("Bearer ", "") if authorization else None
    
    # Obtener datos de los microservicios
    cities = await ms_client.get_all_cities()
    zones = await ms_client.get_all_zones(city_id=city_id)
    sellers = await ms_client.get_all_sellers(token=token)
    shopkeepers = await ms_client.get_all_shopkeepers(token=token)
    
    # Procesar datos por zona
    zones_data = []
    for zone in zones:
        zone_sellers = [s for s in sellers if s.get("zone_id") == zone["id"]]
        
        # Contar tenderos de esta zona (a través de sus vendedores)
        zone_shopkeeper_count = 0
        for seller in zone_sellers:
            seller_shopkeepers = [
                s for s in shopkeepers 
                if s.get("seller_id") == seller["id"]
            ]
            zone_shopkeeper_count += len(seller_shopkeepers)
        
        # Calcular porcentaje de cobertura (arbitrario basado en tenderos)
        max_expected = len(zone_sellers) * 80  # 80 por vendedor
        coverage = (zone_shopkeeper_count / max_expected * 100) if max_expected > 0 else 0
        
        zones_data.append(CoverageByZoneItem(
            zone_id=zone["id"],
            zone_name=zone["name"],
            city_name=zone.get("city_name", ""),
            total_sellers=len(zone_sellers),
            total_shopkeepers=zone_shopkeeper_count,
            coverage_percentage=round(coverage, 2)
        ))
    
    return CoverageReportResponse(
        report_date=datetime.now(),
        total_cities=len(cities) if not city_id else 1,
        total_zones=len(zones),
        total_sellers=len(sellers),
        total_shopkeepers=len(shopkeepers),
        zones=zones_data
    )


@router.get("/sellers-performance", response_model=SellerPerformanceReportResponse)
async def get_sellers_performance(
    zone_id: Optional[int] = Query(None, description="Filtrar por zona"),
    current_user: dict = Depends(get_current_user),
    authorization: str = Header(None)
):
    """
    Reporte de rendimiento de vendedores
    
    Muestra estadísticas de cada vendedor y su carga de trabajo
    """
    token = authorization.replace("Bearer ", "") if authorization else None
    
    sellers = await ms_client.get_all_sellers(zone_id=zone_id, token=token)
    zones = await ms_client.get_all_zones()
    
    sellers_data = []
    total_shopkeepers = 0
    
    for seller in sellers:
        # Obtener tenderos del vendedor
        seller_shopkeepers = await ms_client.get_all_shopkeepers(
            seller_id=seller["id"],
            token=token
        )
        
        shopkeeper_count = seller.get("total_shopkeepers", len(seller_shopkeepers))
        total_shopkeepers += shopkeeper_count
        
        # Obtener nombre de zona
        zone_name = next(
            (z["name"] for z in zones if z["id"] == seller.get("zone_id")),
            "Sin zona"
        )
        
        # Calcular eficiencia (basado en carga de trabajo óptima)
        optimal_load = 40  # 40 tenderos es óptimo
        if shopkeeper_count == 0:
            efficiency = 0
        elif shopkeeper_count <= optimal_load:
            efficiency = (shopkeeper_count / optimal_load) * 100
        else:
            # Penalizar sobrecarga
            efficiency = max(0, 100 - ((shopkeeper_count - optimal_load) / optimal_load) * 50)
        
        sellers_data.append(SellerPerformanceItem(
            seller_id=seller["id"],
            seller_name=seller["name"],
            seller_email=seller["email"],
            zone_name=zone_name,
            total_shopkeepers=shopkeeper_count,
            is_over_limit=shopkeeper_count > 80,
            efficiency_score=round(efficiency, 2)
        ))
    
    avg_shopkeepers = (total_shopkeepers / len(sellers)) if sellers else 0
    
    return SellerPerformanceReportResponse(
        report_date=datetime.now(),
        total_sellers=len(sellers),
        avg_shopkeepers_per_seller=round(avg_shopkeepers, 2),
        sellers=sellers_data
    )


@router.get("/zones/{zone_id}/statistics", response_model=ZoneStatistics)
async def get_zone_statistics(
    zone_id: int,
    current_user: dict = Depends(get_current_user),
    authorization: str = Header(None)
):
    """
    Estadísticas detalladas de una zona específica
    """
    token = authorization.replace("Bearer ", "") if authorization else None
    
    # Obtener zona
    zone = await ms_client.get_zone_by_id(zone_id)
    if not zone:
        raise HTTPException(404, "Zona no encontrada")
    
    # Obtener datos
    sellers = await ms_client.get_all_sellers(zone_id=zone_id, token=token)
    all_shopkeepers = await ms_client.get_all_shopkeepers(token=token)
    unassigned = await ms_client.get_all_shopkeepers(unassigned=True, token=token)
    
    # Contar tenderos de esta zona
    zone_shopkeepers = []
    for seller in sellers:
        seller_shops = [s for s in all_shopkeepers if s.get("seller_id") == seller["id"]]
        zone_shopkeepers.extend(seller_shops)
    
    assigned_count = len(zone_shopkeepers)
    
    # Filtrar unassigned de esta zona (aproximación)
    unassigned_count = len(unassigned)
    
    avg_per_seller = (assigned_count / len(sellers)) if sellers else 0
    
    return ZoneStatistics(
        zone_id=zone["id"],
        zone_name=zone["name"],
        city_name=zone.get("city_name", ""),
        sellers_count=len(sellers),
        shopkeepers_count=assigned_count + unassigned_count,
        assigned_shopkeepers=assigned_count,
        unassigned_shopkeepers=unassigned_count,
        avg_shopkeepers_per_seller=round(avg_per_seller)
    )


@router.get("/metrics", response_model=SystemMetrics)
async def get_system_metrics(
    current_user: dict = Depends(get_current_user),
    authorization: str = Header(None)
):
    """
    Métricas generales del sistema
    
    Dashboard con indicadores clave
    """
    token = authorization.replace("Bearer ", "") if authorization else None
    
    # Obtener todos los datos
    cities = await ms_client.get_all_cities()
    zones = await ms_client.get_all_zones()
    sellers = await ms_client.get_all_sellers(token=token)
    shopkeepers = await ms_client.get_all_shopkeepers(token=token)
    unassigned = await ms_client.get_all_shopkeepers(unassigned=True, token=token)
    assignments = await ms_client.get_all_assignments(token=token)
    
    # Calcular métricas
    total_cities = len(cities)
    total_zones = len(zones)
    total_sellers = len(sellers)
    total_shopkeepers = len(shopkeepers)
    active_assignments = len([a for a in assignments if a.get("is_active", True)])
    unassigned_count = len(unassigned)
    
    avg_shopkeepers = (active_assignments / total_sellers) if total_sellers > 0 else 0
    
    # Determinar salud del sistema
    if total_sellers > 0 and total_shopkeepers > 0:
        health_status = "healthy"
    else:
        health_status = "warning"
    
    return SystemMetrics(
        total_cities=total_cities,
        total_zones=total_zones,
        total_sellers=total_sellers,
        total_shopkeepers=total_shopkeepers,
        active_assignments=active_assignments,
        unassigned_shopkeepers=unassigned_count,
        avg_shopkeepers_per_seller=round(avg_shopkeepers),
        system_health=health_status
    )


@router.post("/export")
async def export_report(
    export_request: ExportRequest,
    current_user: dict = Depends(get_current_user),
    authorization: str = Header(None)
):
    """
    Exportar reporte en diferentes formatos (CSV, JSON)
    """
    token = authorization.replace("Bearer ", "") if authorization else None
    
    # Obtener datos según el tipo de reporte
    if export_request.report_type == "coverage":
        # Generar reporte de cobertura
        zones = await ms_client.get_all_zones(city_id=export_request.city_id)
        sellers = await ms_client.get_all_sellers(token=token)
        
        data = []
        for zone in zones:
            zone_sellers = [s for s in sellers if s.get("zone_id") == zone["id"]]
            data.append({
                "zone_id": zone["id"],
                "zone_name": zone["name"],
                "city_name": zone.get("city_name", ""),
                "total_sellers": len(zone_sellers)
            })
    
    elif export_request.report_type == "sellers":
        # Generar reporte de vendedores
        sellers = await ms_client.get_all_sellers(token=token)
        data = sellers
    
    else:
        raise HTTPException(400, f"Tipo de reporte no soportado: {export_request.report_type}")
    
    # Exportar según formato
    if export_request.format == "csv":
        output = io.StringIO()
        if data:
            writer = csv.DictWriter(output, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={export_request.report_type}.csv"}
        )
    
    elif export_request.format == "json":
        return StreamingResponse(
            iter([json.dumps(data, indent=2)]),
            media_type="application/json",
            headers={"Content-Disposition": f"attachment; filename={export_request.report_type}.json"}
        )
    
    else:
        raise HTTPException(400, f"Formato no soportado: {export_request.format}")


@router.get("/sales-comparison", response_model=SalesComparisonResponse)
async def get_sales_comparison(
    comparison_type: str = Query("both", description="Tipo de comparación: zones, cities, both"),
    current_user: dict = Depends(get_current_user),
    authorization: str = Header(None)
):
    """
    Comparación de ventas entre zonas y ciudades
    
    Detecta áreas de mayor desempeño basado en:
    - Número de tenderos asignados (actividad de mercado)
    - Número de vendedores
    - Score de desempeño calculado
    """
    token = authorization.replace("Bearer ", "") if authorization else None
    allowed_types = {"zones", "cities", "both"}
    if comparison_type not in allowed_types:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail=f"comparison_type debe ser uno de {', '.join(sorted(allowed_types))}"
        )
    
    # Obtener datos de los microservicios
    cities = await ms_client.get_all_cities()
    zones = await ms_client.get_all_zones()
    sellers = await ms_client.get_all_sellers(token=token)
    assignments = await ms_client.get_all_assignments(token=token)
    
    # Filtrar solo asignaciones activas
    active_assignments = [a for a in assignments if a.get("is_active", True)]
    
    # Calcular datos por zona
    zones_data = []
    for zone in zones:
        # Obtener vendedores de esta zona
        zone_sellers = [s for s in sellers if s.get("zone_id") == zone["id"]]
        
        # Contar tenderos asignados a esta zona
        zone_seller_ids = [s["id"] for s in zone_sellers]
        zone_shopkeepers_count = len([
            a for a in active_assignments 
            if a.get("seller_id") in zone_seller_ids
        ])
        
        # Calcular score de desempeño
        # Basado en: tenderos asignados, número de vendedores
        sellers_count = len(zone_sellers)
        if sellers_count > 0:
            avg_shopkeepers = zone_shopkeepers_count / sellers_count
            # Score basado en penetración de mercado (promedio de tenderos por vendedor)
            performance_score = min(100, (avg_shopkeepers / 50) * 100)  # 50 tenderos = 100%
            market_penetration = round(avg_shopkeepers, 2)
        else:
            performance_score = 0
            market_penetration = 0
        
        # Obtener nombre de ciudad
        city_name = next(
            (c["name"] for c in cities if c["id"] == zone.get("city_id")),
            "Desconocida"
        )
        
        zones_data.append(SalesComparisonItem(
            zone_id=zone["id"],
            zone_name=zone["name"],
            city_id=zone.get("city_id", 0),
            city_name=city_name,
            total_shopkeepers=zone_shopkeepers_count,
            total_sellers=sellers_count,
            performance_score=round(performance_score, 2),
            market_penetration=market_penetration
        ))
    
    # Calcular datos por ciudad
    cities_data = []
    for city in cities:
        city_zones = [z for z in zones if z.get("city_id") == city["id"]]
        
        city_sellers = [
            s for s in sellers 
            if s.get("zone_id") in [z["id"] for z in city_zones]
        ]
        
        city_seller_ids = [s["id"] for s in city_sellers]
        city_shopkeepers_count = len([
            a for a in active_assignments 
            if a.get("seller_id") in city_seller_ids
        ])
        
        total_zones = len(city_zones)
        sellers_count = len(city_sellers)
        
        if sellers_count > 0:
            avg_shopkeepers = city_shopkeepers_count / sellers_count
            performance_score = min(100, (avg_shopkeepers / 50) * 100)
            market_penetration = round(avg_shopkeepers, 2)
        else:
            performance_score = 0
            market_penetration = 0
        
        cities_data.append(CitySalesComparisonItem(
            city_id=city["id"],
            city_name=city["name"],
            total_zones=total_zones,
            total_shopkeepers=city_shopkeepers_count,
            total_sellers=sellers_count,
            performance_score=round(performance_score, 2),
            market_penetration=market_penetration
        ))
    
    # Identificar top performers (top 3 por tipo)
    sorted_zones = sorted(zones_data, key=lambda x: x.performance_score, reverse=True)
    top_zones = sorted_zones[:3]
    sorted_cities = sorted(cities_data, key=lambda x: x.performance_score, reverse=True)
    top_cities = sorted_cities[:3]
    
    # Determinar tipo de datos a retornar
    if comparison_type == "zones":
        response_zones = zones_data
        response_cities = []
        response_top_zones = top_zones
        response_top_cities = []
    elif comparison_type == "cities":
        response_zones = []
        response_cities = cities_data
        response_top_zones = []
        response_top_cities = top_cities
    else:
        response_zones = zones_data
        response_cities = cities_data
        response_top_zones = top_zones
        response_top_cities = top_cities
    
    return SalesComparisonResponse(
        report_date=datetime.now(),
        comparison_type=comparison_type,
        zones=response_zones,
        cities=response_cities,
        top_zones=response_top_zones,
        top_cities=response_top_cities
    )


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check del servicio de reportes"""
    
    # Verificar estado de servicios externos
    geo_status = await ms_client.check_service_health(settings.MS_GEO_URL)
    user_status = await ms_client.check_service_health(settings.MS_USER_URL)
    auth_status = await ms_client.check_service_health(settings.MS_AUTH_URL)
    
    external_services = {
        "ms-geo": geo_status,
        "ms-user": user_status,
        "ms-auth": auth_status
    }
    
    # Estado general
    all_healthy = all(s == "connected" for s in external_services.values())
    status_value = "healthy" if all_healthy else "degraded"
    
    return HealthResponse(
        status=status_value,
        service=settings.APP_NAME,
        version=settings.APP_VERSION,
        external_services=external_services
    )

