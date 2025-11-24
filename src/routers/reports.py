"""
Router de Reportes y Análisis
"""
from fastapi import APIRouter, Depends, HTTPException, status, Query, Header
from fastapi.responses import StreamingResponse
from typing import Optional, List, Dict
from datetime import datetime, timedelta, date
import io
import csv
import json
import asyncio
from collections import defaultdict
from random import Random

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
    CitySalesComparisonItem,
    SalesHistoryResponse,
    SaleRecord,
    SalesSummary,
    VisitsComplianceResponse,
    SellerComplianceItem,
    ComplianceSummary,
    PeriodInfo,
    TopProductItem,
    TopProductsResponse,
    MissingPopularProductItem,
    ZoneDemandProduct,
    ZoneDemandInsight,
    DemandTrends,
    DemandTrendPoint,
    DemandForecastPoint,
    StrategicRecommendation,
    MarketOpportunitiesSummary,
    MarketOpportunitiesResponse,
    SellerAggregatedSalesResponse,
    ShopkeeperSalesSummary
)
from ..utils import get_current_user, ms_client
from ..config import settings

router = APIRouter()


MOCK_PRODUCTS = [
    {"name": "Arroz Premium 1Kg", "category": "Granos", "price": 5800.0, "max_quantity": 18},
    {"name": "Aceite Vegetal 1L", "category": "Aceites", "price": 9200.0, "max_quantity": 12},
    {"name": "Bebida Energética 500ml", "category": "Bebidas", "price": 4500.0, "max_quantity": 24},
    {"name": "Galletas Integrales 12u", "category": "Snacks", "price": 7200.0, "max_quantity": 15},
    {"name": "Detergente Líquido 2L", "category": "Aseo", "price": 11800.0, "max_quantity": 8},
    {"name": "Papel Higiénico 12 rollos", "category": "Aseo", "price": 16300.0, "max_quantity": 6},
    {"name": "Café Molido 500g", "category": "Bebidas", "price": 9800.0, "max_quantity": 10},
    {"name": "Azúcar Refinada 1Kg", "category": "Granos", "price": 4600.0, "max_quantity": 20}
]

MOCK_STATUSES = ["Completada", "Pagada", "Pendiente"]


def _generate_mock_sales(shopkeeper_id: int, rng: Random) -> list[dict]:
    """Genera ventas determinísticas basadas en el ID del tendero."""
    base_datetime = datetime.utcnow().replace(microsecond=0)
    num_records = rng.randint(6, 14)
    
    sales = []
    for index in range(num_records):
        product = rng.choice(MOCK_PRODUCTS)
        quantity = rng.randint(1, product["max_quantity"])
        
        days_ago = rng.randint(0, 60)
        hours_offset = rng.randint(0, 23)
        sold_at = base_datetime - timedelta(days=days_ago, hours=hours_offset)
        
        sale_id = shopkeeper_id * 1000 + index + 1
        invoice_number = f"INV-{shopkeeper_id:03d}-{sold_at.strftime('%Y%m%d')}-{index + 1:02d}"
        status = rng.choices(MOCK_STATUSES, weights=[0.7, 0.2, 0.1], k=1)[0]
        
        sales.append(
            {
                "sale_id": sale_id,
                "invoice_number": invoice_number,
                "product_name": product["name"],
                "category": product["category"],
                "quantity": quantity,
                "unit_price": product["price"],
                "total_amount": round(quantity * product["price"], 2),
                "sold_at": sold_at,
                "status": status if status != "Pagada" else "Completada"
            }
        )
    
    sales.sort(key=lambda item: item["sold_at"], reverse=True)
    return sales


def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    """Convierte cadenas ISO (con o sin Z) a datetime."""
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        # Reemplazar Z por +00:00 para compatibilidad con fromisoformat
        normalized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    except Exception:
        return None


def _safe_float(value, default: float = 0.0) -> float:
    """Convierte valores a float manejando None/Decimal/str."""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


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


@router.get("/sales/top-products", response_model=TopProductsResponse)
async def get_top_products_by_zone(
    limit: int = Query(3, ge=1, le=10, description="Cantidad de productos en el ranking"),
    seller_id: Optional[int] = Query(None, description="Filtrar por vendedor y su zona"),
    zone_id: Optional[int] = Query(None, description="Zona específica"),
    current_user: dict = Depends(get_current_user),
    authorization: str = Header(None)
):
    """
    Ranking de productos con mayor rotación por zona (HU10)

    Se determina la demanda a partir de la necesidad de reposición
    agregada en los inventarios de los tenderos de la zona.
    """
    token = authorization.replace("Bearer ", "") if authorization else None
    
    if not zone_id and not seller_id:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            "Debes proporcionar seller_id o zone_id para generar el ranking"
        )
    
    seller_info = None
    seller_name = None
    if seller_id:
        seller_info = await ms_client.get_seller_by_id(seller_id, token=token)
        if not seller_info:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "Vendedor no encontrado")
        seller_name = seller_info.get("name")
        if not zone_id:
            zone_id = seller_info.get("zone_id")
    
    if not zone_id:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            "No se pudo determinar la zona del vendedor"
        )
    
    zone = await ms_client.get_zone_by_id(zone_id)
    if not zone:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Zona no encontrada")
    
    # Obtener todos los vendedores de la zona
    zone_sellers = await ms_client.get_all_sellers(zone_id=zone_id, token=token)
    if not zone_sellers and seller_info:
        zone_sellers = [seller_info]
    
    shopkeeper_ids = set()
    for zone_seller in zone_sellers:
        sid = zone_seller.get("id")
        if not sid:
            continue
        shopkeepers = await ms_client.get_all_shopkeepers(seller_id=sid, token=token)
        for shopkeeper in shopkeepers:
            if shopkeeper.get("id"):
                shopkeeper_ids.add(shopkeeper["id"])
    
    total_shopkeepers = len(shopkeeper_ids)
    product_stats = {}
    
    for shopkeeper_id in shopkeeper_ids:
        inventory_items = await ms_client.get_inventory_by_shopkeeper(shopkeeper_id, token=token)
        for item in inventory_items:
            product_id = item.get("product_id")
            if not product_id:
                continue
            
            stock = float(item.get("stock", item.get("current_stock", 0)) or 0)
            max_stock = float(item.get("max_stock") or 0)
            min_stock = float(item.get("min_stock") or 0)
            price = float(item.get("price", item.get("unit_price", 0)) or 0)
            units_needed = max(0.0, max_stock - stock)
            
            stats = product_stats.setdefault(product_id, {
                "product_name": item.get("product_name") or f"Producto {product_id}",
                "category": item.get("category") or item.get("product_category") or "Sin categoría",
                "total_units_needed": 0.0,
                "total_stock": 0.0,
                "total_price": 0.0,
                "shopkeepers": 0,
                "low_stock": 0,
            })
            
            # Actualizar nombre/categoría si llegan datos más completos
            if item.get("product_name"):
                stats["product_name"] = item["product_name"]
            if item.get("category") or item.get("product_category"):
                stats["category"] = item.get("category") or item.get("product_category")
            
            stats["total_units_needed"] += units_needed
            stats["total_stock"] += stock
            stats["total_price"] += price
            stats["shopkeepers"] += 1
            if min_stock > 0 and stock < min_stock:
                stats["low_stock"] += 1
    
    total_products = len(product_stats)
    if total_products == 0:
        return TopProductsResponse(
            zone_id=zone_id,
            zone_name=zone.get("name", f"Zona {zone_id}"),
            seller_id=seller_id,
            seller_name=seller_name,
            total_shopkeepers=total_shopkeepers,
            total_products=0,
            generated_at=datetime.now(),
            items=[]
        )
    
    sorted_products = sorted(
        product_stats.items(),
        key=lambda item: (
            -item[1]["total_units_needed"],
            -item[1]["low_stock"],
            -item[1]["total_stock"]
        )
    )
    
    items: List[TopProductItem] = []
    for rank, (product_id, stats) in enumerate(sorted_products[:limit], start=1):
        avg_price = stats["total_price"] / stats["shopkeepers"] if stats["shopkeepers"] else 0.0
        items.append(TopProductItem(
            rank=rank,
            product_id=product_id,
            product_name=stats["product_name"],
            category=stats["category"],
            total_units_needed=round(stats["total_units_needed"], 2),
            total_current_stock=round(stats["total_stock"], 2),
            avg_unit_price=round(avg_price, 2),
            shopkeepers_count=stats["shopkeepers"],
            low_stock_shopkeepers=stats["low_stock"]
        ))
    
    return TopProductsResponse(
        zone_id=zone_id,
        zone_name=zone.get("name", f"Zona {zone_id}"),
        seller_id=seller_id,
        seller_name=seller_name,
        total_shopkeepers=total_shopkeepers,
        total_products=total_products,
        generated_at=datetime.now(),
        items=items
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


@router.get(
    "/sales-history/shopkeepers/{shopkeeper_id}",
    response_model=SalesHistoryResponse,
    summary="Historial de ventas por tendero",
    description="Retorna el historial de ventas y métricas agregadas para un tendero específico."
)
async def get_sales_history(
    shopkeeper_id: int,
    start_date: date = Query(
        None,
        description="Fecha inicial del rango (YYYY-MM-DD). Por defecto últimos 30 días."
    ),
    end_date: date = Query(
        None,
        description="Fecha final del rango (YYYY-MM-DD)."
    ),
    current_user: dict = Depends(get_current_user),
    authorization: str = Header(None)
):
    token = authorization.replace("Bearer ", "") if authorization else None
    
    date_end = end_date or datetime.utcnow().date()
    date_start = start_date or (date_end - timedelta(days=30))
    
    if date_start > date_end:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail="La fecha inicial no puede ser posterior a la fecha final."
        )
    
    # Obtener información del tendero y su vendedor
    shopkeeper = await ms_client.get_shopkeeper_by_id(shopkeeper_id, token=token)
    if not shopkeeper:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Tendero no encontrado.")
    
    seller_info = None
    seller_id = shopkeeper.get("seller_id")
    if seller_id:
        seller_info = await ms_client.get_seller_by_id(seller_id, token=token)
    
    rng = Random(shopkeeper_id)
    generated_sales = _generate_mock_sales(shopkeeper_id, rng)
    
    filtered_sales = [
        sale for sale in generated_sales
        if date_start <= sale["sold_at"].date() <= date_end
    ]
    
    total_records = len(filtered_sales)
    total_units = sum(sale["quantity"] for sale in filtered_sales)
    total_amount = round(sum(sale["total_amount"] for sale in filtered_sales), 2)
    average_ticket = round(total_amount / total_records, 2) if total_records else 0.0
    
    sales_payload = [
        SaleRecord(
            sale_id=sale["sale_id"],
            invoice_number=sale["invoice_number"],
            product_name=sale["product_name"],
            category=sale["category"],
            quantity=sale["quantity"],
            unit_price=sale["unit_price"],
            total_amount=sale["total_amount"],
            sold_at=sale["sold_at"],
            status=sale["status"]
        )
        for sale in filtered_sales
    ]
    
    return SalesHistoryResponse(
        report_generated_at=datetime.utcnow(),
        shopkeeper_id=shopkeeper_id,
        shopkeeper_name=shopkeeper.get("name"),
        shopkeeper_business_name=shopkeeper.get("business_name"),
        seller_id=seller_info.get("id") if seller_info else shopkeeper.get("seller_id"),
        seller_name=(
            seller_info.get("name")
            if seller_info
            else shopkeeper.get("seller_name")
        ),
        range_start=date_start,
        range_end=date_end,
        summary=SalesSummary(
            total_records=total_records,
            total_units=total_units,
            total_amount=total_amount,
            average_ticket=average_ticket
        ),
        sales=sales_payload
    )


@router.get("/market-opportunities", response_model=MarketOpportunitiesResponse)
async def get_market_opportunities(
    city_id: Optional[int] = Query(None, description="Filtrar por ciudad"),
    zone_id: Optional[int] = Query(None, description="Filtrar por zona"),
    category: Optional[str] = Query(None, description="Filtrar por categoría de producto"),
    start_date: Optional[datetime] = Query(None, description="Fecha de inicio para tendencias"),
    end_date: Optional[datetime] = Query(None, description="Fecha de fin para tendencias"),
    popularity_threshold: float = Query(
        0.6,
        ge=0.0,
        le=1.0,
        description="Umbral mínimo de popularidad (0-1)"
    ),
    min_missing_shopkeepers: int = Query(
        3,
        ge=1,
        le=500,
        description="Mínimo de tenderos sin el producto para considerarlo brecha"
    ),
    current_user: dict = Depends(get_current_user),
    authorization: str = Header(None)
):
    """
    HU19 - Análisis de oportunidades de mercado.
    
    Identifica productos populares ausentes, tendencias por zona y genera recomendaciones estratégicas.
    """
    if current_user.get("role") != "ADMIN":
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "Solo los administradores pueden acceder a este análisis"
        )
    
    token = None
    if authorization:
        token = authorization.replace("Bearer ", "").strip()
    
    def _to_str(value):
        return None if value is None else str(value)

    filters_snapshot = {
        "city_id": _to_str(city_id),
        "zone_id": _to_str(zone_id),
        "category": _to_str(category),
        "start_date": start_date.isoformat() if start_date else None,
        "end_date": end_date.isoformat() if end_date else None,
        "popularity_threshold": _to_str(popularity_threshold),
        "min_missing_shopkeepers": _to_str(min_missing_shopkeepers)
    }
    
    threshold = max(0.0, min(popularity_threshold, 1.0))
    effective_min_missing = max(1, min_missing_shopkeepers)
    
    cities, zones, sellers, shopkeepers, products = await asyncio.gather(
        ms_client.get_all_cities(),
        ms_client.get_all_zones(),
        ms_client.get_all_sellers(token=token),
        ms_client.get_all_shopkeepers(token=token),
        ms_client.get_all_products(category=category)
    )
    
    city_map = {city["id"]: city for city in cities}
    zone_map = {zone["id"]: zone for zone in zones}
    zone_ids_for_city = {
        zone["id"] for zone in zones
        if city_id is None or zone.get("city_id") == city_id
    }
    
    filtered_sellers = []
    for seller in sellers:
        seller_zone_id = seller.get("zone_id")
        if zone_id and seller_zone_id != zone_id:
            continue
        if city_id and seller_zone_id not in zone_ids_for_city:
            continue
        filtered_sellers.append(seller)
    
    if not filtered_sellers and not zone_id and not city_id:
        filtered_sellers = sellers
    
    seller_ids = {seller["id"] for seller in filtered_sellers if seller.get("id")}
    if not seller_ids:
        empty_summary = MarketOpportunitiesSummary(
            generated_at=datetime.utcnow(),
            total_products_missing=0,
            total_impacted_shopkeepers=0,
            estimated_monthly_revenue=0.0
        )
        return MarketOpportunitiesResponse(
            summary=empty_summary,
            filters=filters_snapshot,
            missing_popular_products=[],
            zone_trends=[],
            demand_trends=DemandTrends(timeline=[], forecast=[]),
            recommendations=[]
        )
    
    filtered_shopkeepers = [
        shopkeeper for shopkeeper in shopkeepers
        if shopkeeper.get("seller_id") in seller_ids
    ]
    
    if not filtered_shopkeepers:
        empty_summary = MarketOpportunitiesSummary(
            generated_at=datetime.utcnow(),
            total_products_missing=0,
            total_impacted_shopkeepers=0,
            estimated_monthly_revenue=0.0
        )
        return MarketOpportunitiesResponse(
            summary=empty_summary,
            filters=filters_snapshot,
            missing_popular_products=[],
            zone_trends=[],
            demand_trends=DemandTrends(timeline=[], forecast=[]),
            recommendations=[]
        )
    
    total_shopkeepers = len(filtered_shopkeepers)
    product_catalog = {product["id"]: product for product in products if product.get("id")}
    seller_zone_map = {seller["id"]: seller.get("zone_id") for seller in filtered_sellers}
    
    shop_zone_details: Dict[int, Dict[str, Optional[str]]] = {}
    for shop in filtered_shopkeepers:
        zone_info = {"zone_id": None, "zone_name": "Sin zona", "city_name": None}
        seller_id = shop.get("seller_id")
        zone_id_for_shop = seller_zone_map.get(seller_id)
        if zone_id_for_shop:
            zone_data = zone_map.get(zone_id_for_shop, {})
            city_data = city_map.get(zone_data.get("city_id"))
            zone_info = {
                "zone_id": zone_id_for_shop,
                "zone_name": zone_data.get("name", f"Zona {zone_id_for_shop}"),
                "city_name": city_data.get("name") if city_data else None
            }
        shop_zone_details[shop["id"]] = zone_info
    
    inventory_by_shop = {}
    for shop in filtered_shopkeepers:
        shop_id = shop.get("id")
        if not shop_id:
            continue
        inventory_items = await ms_client.get_inventory_by_shopkeeper(shop_id, token=token)
        inventory_by_shop[shop_id] = inventory_items or []
    
    product_stats = {}
    shop_products_map: Dict[int, set] = {}
    
    for shop in filtered_shopkeepers:
        shop_id = shop.get("id")
        inventory_items = inventory_by_shop.get(shop_id, [])
        product_ids_for_shop = set()
        
        for item in inventory_items:
            product_id = item.get("product_id") or item.get("id")
            if not product_id:
                continue
            
            catalog_entry = product_catalog.get(product_id, {})
            product_category = item.get("category") or item.get("product_category") or catalog_entry.get("category")
            if category and product_category and category.lower() not in product_category.lower():
                continue
            
            product_ids_for_shop.add(product_id)
            
            product_name = (
                item.get("product_name")
                or catalog_entry.get("name")
                or f"Producto {product_id}"
            )
            unit_price = (
                _safe_float(item.get("price"))
                or _safe_float(item.get("unit_price"))
                or _safe_float(catalog_entry.get("price"))
            )
            stock = _safe_float(item.get("stock", item.get("current_stock")))
            min_stock = _safe_float(item.get("min_stock"), default=0.0)
            max_stock = _safe_float(item.get("max_stock"), default=min_stock * 2 if min_stock else stock + 10)
            units_needed = max(0.0, max_stock - stock)
            
            stats = product_stats.setdefault(product_id, {
                "product_name": product_name,
                "category": product_category,
                "shopkeepers_with": 0,
                "total_stock": 0.0,
                "total_units_needed": 0.0,
                "total_price": 0.0,
                "low_stock": 0,
                "shopkeeper_ids": set()
            })
            
            stats["product_name"] = product_name
            stats["category"] = product_category
            stats["shopkeepers_with"] += 1
            stats["total_stock"] += stock
            stats["total_units_needed"] += units_needed
            stats["total_price"] += unit_price if unit_price else 0.0
            stats["shopkeeper_ids"].add(shop_id)
            if min_stock and stock < min_stock:
                stats["low_stock"] += 1
        
        shop_products_map[shop_id] = product_ids_for_shop
    
    missing_products: List[MissingPopularProductItem] = []
    for product_id, stats in product_stats.items():
        popularity = stats["shopkeepers_with"] / total_shopkeepers if total_shopkeepers else 0.0
        if popularity < threshold:
            continue
        
        missing_count = total_shopkeepers - stats["shopkeepers_with"]
        if missing_count < effective_min_missing:
            continue
        
        avg_price = (
            stats["total_price"] / stats["shopkeepers_with"]
            if stats["shopkeepers_with"] else 0.0
        )
        avg_units_needed = (
            stats["total_units_needed"] / stats["shopkeepers_with"]
            if stats["shopkeepers_with"] else 0.0
        )
        potential_revenue = max(0.0, missing_count * max(avg_units_needed, 1.0) * avg_price)
        
        impacted_zones = set()
        for shop in filtered_shopkeepers:
            shop_id = shop.get("id")
            if not shop_id:
                continue
            if product_id not in shop_products_map.get(shop_id, set()):
                impacted_zones.add(shop_zone_details[shop_id]["zone_name"])
        
        if not impacted_zones:
            continue
        
        priority = "low"
        if popularity >= 0.8 and missing_count >= effective_min_missing * 2:
            priority = "high"
        elif popularity >= 0.65:
            priority = "medium"
        
        missing_products.append(MissingPopularProductItem(
            product_id=product_id,
            product_name=stats["product_name"],
            category=stats["category"],
            global_popularity=round(popularity, 2),
            missing_shopkeepers=missing_count,
            impacted_zones=sorted(impacted_zones),
            potential_monthly_revenue=round(potential_revenue, 2),
            priority=priority,
            avg_unit_price=round(avg_price, 2)
        ))
    
    missing_products.sort(
        key=lambda item: (
            {"high": 2, "medium": 1, "low": 0}[item.priority],
            item.global_popularity,
            item.potential_monthly_revenue
        ),
        reverse=True
    )
    
    zone_groups = defaultdict(list)
    for shop in filtered_shopkeepers:
        details = shop_zone_details.get(shop["id"], {})
        zone_groups[details.get("zone_id")].append(shop["id"])
    
    zone_trends: List[ZoneDemandInsight] = []
    for group_zone_id, shop_ids in zone_groups.items():
        zone_product_stats = {}
        total_gap = 0.0
        
        for shop_id in shop_ids:
            for item in inventory_by_shop.get(shop_id, []):
                product_id = item.get("product_id") or item.get("id")
                if not product_id:
                    continue
                stats = zone_product_stats.setdefault(product_id, {
                    "name": item.get("product_name") or f"Producto {product_id}",
                    "units_needed": 0.0,
                    "low_stock": 0,
                    "shopkeepers": 0
                })
                stock = _safe_float(item.get("stock", item.get("current_stock")))
                min_stock = _safe_float(item.get("min_stock"))
                max_stock = _safe_float(item.get("max_stock"), default=min_stock * 2 if min_stock else stock + 10)
                units_needed = max(0.0, max_stock - stock)
                stats["units_needed"] += units_needed
                stats["shopkeepers"] += 1
                if min_stock and stock < min_stock:
                    stats["low_stock"] += 1
                total_gap += units_needed
        
        zone_data = zone_map.get(group_zone_id or 0, {})
        city_name = None
        if group_zone_id and zone_data.get("city_id"):
            city_name = city_map.get(zone_data.get("city_id"), {}).get("name")
        
        top_demands = sorted(
            zone_product_stats.items(),
            key=lambda entry: entry[1]["units_needed"],
            reverse=True
        )[:3]
        
        zone_products = [
            ZoneDemandProduct(
                product_id=pid,
                product_name=stats["name"],
                growth_percentage=round(
                    min(100.0, (stats["low_stock"] / stats["shopkeepers"] * 100) if stats["shopkeepers"] else 0.0),
                    2
                ),
                stock_gap=round(stats["units_needed"] / stats["shopkeepers"], 2) if stats["shopkeepers"] else 0.0
            )
            for pid, stats in top_demands
        ]
        
        avg_stock_gap = (total_gap / len(shop_ids)) if shop_ids else 0.0
        demand_variation = 0.0
        if zone_products:
            demand_variation = round(
                sum(product.growth_percentage for product in zone_products) / len(zone_products),
                2
            )
        
        zone_trends.append(ZoneDemandInsight(
            zone_id=group_zone_id,
            zone_name=zone_data.get("name", "Sin zona"),
            city_name=city_name,
            top_demands=zone_products,
            avg_stock_gap=round(avg_stock_gap, 2),
            demand_variation=demand_variation,
            shopkeepers_covered=len(shop_ids),
            unmet_demand=round(total_gap, 2)
        ))
    
    zone_trends.sort(key=lambda item: item.unmet_demand, reverse=True)
    
    demand_timeline = defaultdict(lambda: {"total": 0, "completed": 0, "pending": 0})
    all_visits = []
    for seller_id in seller_ids:
        visits = await ms_client.get_visits(
            seller_id=seller_id,
            start_date=start_date,
            end_date=end_date,
            token=token
        )
        if visits:
            all_visits.extend(visits)
    
    for visit in all_visits:
        scheduled_dt = _parse_iso_datetime(visit.get("scheduled_date"))
        if not scheduled_dt:
            continue
        label = f"{scheduled_dt.isocalendar().year}-W{scheduled_dt.isocalendar().week:02d}"
        demand_timeline[label]["total"] += 1
        status_visit = (visit.get("status") or "").lower()
        if status_visit == "completed":
            demand_timeline[label]["completed"] += 1
        elif status_visit == "pending":
            demand_timeline[label]["pending"] += 1
    
    timeline_points = [
        DemandTrendPoint(
            label=label,
            total_demand=data["total"],
            completed=data["completed"],
            pending=data["pending"]
        )
        for label, data in sorted(demand_timeline.items())
    ]
    
    forecast_points: List[DemandForecastPoint] = []
    if timeline_points:
        demands = [point.total_demand for point in timeline_points]
        avg_demand = sum(demands) / len(demands) if demands else 0
        momentum = 0.0
        if len(demands) >= 2:
            previous = max(demands[-2], 1)
            momentum = (demands[-1] - demands[-2]) / previous
        
        last_label = timeline_points[-1].label
        try:
            year_str, week_str = last_label.split("-W")
            base_date = datetime.fromisocalendar(int(year_str), int(week_str), 1)
        except Exception:
            base_date = datetime.utcnow()
        
        for offset in (1, 2):
            future_date = base_date + timedelta(weeks=offset)
            iso = future_date.isocalendar()
            label = f"{iso.year}-W{iso.week:02d}"
            growth_factor = max(0.0, momentum) * offset
            expected = max(1, round(avg_demand * (1 + growth_factor)))
            forecast_points.append(DemandForecastPoint(label=label, expected_demand=expected))
    
    demand_trends = DemandTrends(
        timeline=timeline_points,
        forecast=forecast_points
    )
    
    recommendations: List[StrategicRecommendation] = []
    for product in missing_products[:3]:
        recommendations.append(StrategicRecommendation(
            id=f"PROD-{product.product_id}",
            type="supply",
            message=f"Abastecer {product.product_name} en {len(product.impacted_zones)} zonas prioritarias",
            rationale=f"{product.missing_shopkeepers} tenderos sin stock y popularidad {product.global_popularity * 100:.1f}%",
            impact="Alta" if product.priority == "high" else ("Media" if product.priority == "medium" else "Baja"),
            urgency="Alta" if product.priority == "high" else ("Media" if product.priority == "medium" else "Baja")
        ))
    
    if zone_trends:
        top_zone = zone_trends[0]
        recommendations.append(StrategicRecommendation(
            id=f"ZONE-{top_zone.zone_id or 'NA'}",
            type="campaign",
            message=f"Ejecutar campaña en {top_zone.zone_name} para balancear demanda",
            rationale=f"Gap promedio de {top_zone.avg_stock_gap} unidades y variación {top_zone.demand_variation}%",
            impact="Alta" if top_zone.avg_stock_gap > 20 else "Media",
            urgency="Media" if top_zone.avg_stock_gap > 10 else "Baja"
        ))
    
    if demand_trends.forecast:
        forecast = demand_trends.forecast[0]
        recommendations.append(StrategicRecommendation(
            id="TREND-FORECAST",
            type="alert",
            message=f"Preparar capacidad para demanda esperada de {forecast.expected_demand} visitas en {forecast.label}",
            rationale="La tendencia semanal muestra crecimiento sostenido",
            impact="Media",
            urgency="Media"
        ))
    
    recommendations = recommendations[:5]
    
    summary = MarketOpportunitiesSummary(
        generated_at=datetime.utcnow(),
        total_products_missing=len(missing_products),
        total_impacted_shopkeepers=sum(item.missing_shopkeepers for item in missing_products),
        estimated_monthly_revenue=round(
            sum(item.potential_monthly_revenue for item in missing_products),
            2
        )
    )
    
    return MarketOpportunitiesResponse(
        summary=summary,
        filters=filters_snapshot,
        missing_popular_products=missing_products,
        zone_trends=zone_trends,
        demand_trends=demand_trends,
        recommendations=recommendations
    )


@router.get("/visits-compliance", response_model=VisitsComplianceResponse)
async def get_visits_compliance(
    seller_id: Optional[int] = Query(None, description="Filtrar por vendedor específico"),
    zone_id: Optional[int] = Query(None, description="Filtrar por zona geográfica"),
    start_date: Optional[datetime] = Query(None, description="Fecha de inicio del período"),
    end_date: Optional[datetime] = Query(None, description="Fecha de fin del período"),
    sort_by: Optional[str] = Query("compliance_percentage", description="Campo para ordenar (compliance_percentage, seller_name, total_visits)"),
    sort_order: Optional[str] = Query("desc", description="Orden (asc, desc)"),
    current_user: dict = Depends(get_current_user),
    authorization: str = Header(None)
):
    """
    Reporte de cumplimiento de visitas por vendedor
    HU15: Como administrador, quiero ver % de cumplimiento de visitas para evaluar desempeño de cada vendedor
    
    Calcula el porcentaje de cumplimiento basado en:
    - Visitas completadas / (Visitas completadas + Visitas pendientes)
    - Las visitas canceladas no se incluyen en el cálculo
    """
    # Validar permisos (solo ADMIN)
    if current_user.get("role") != "ADMIN":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Solo los administradores pueden ver este reporte"
        )
    
    # Extraer token del header de manera robusta
    token = None
    if authorization:
        # Manejar tanto "Bearer token" como solo "token"
        if authorization.startswith("Bearer "):
            token = authorization.replace("Bearer ", "").strip()
        else:
            token = authorization.strip()
    
    # Obtener todos los vendedores (filtrados por zona si aplica)
    sellers = await ms_client.get_all_sellers(zone_id=zone_id, token=token)
    zones = await ms_client.get_all_zones()
    
    # Si se especifica un vendedor, filtrar
    if seller_id:
        sellers = [s for s in sellers if s["id"] == seller_id]
    
    # Obtener todas las visitas para los vendedores seleccionados
    all_visits = []
    for seller in sellers:
        visits = await ms_client.get_visits(
            seller_id=seller["id"],
            start_date=start_date,
            end_date=end_date,
            token=token
        )
        all_visits.extend(visits)
    
    # Procesar datos por vendedor
    sellers_compliance = []
    total_visits_all = 0
    total_completed_all = 0
    total_pending_all = 0
    total_cancelled_all = 0
    compliance_percentages = []
    
    for seller in sellers:
        # Filtrar visitas de este vendedor
        seller_visits = [v for v in all_visits if v.get("seller_id") == seller["id"]]
        
        # Contar por estado
        completed = len([v for v in seller_visits if v.get("status") == "completed"])
        pending = len([v for v in seller_visits if v.get("status") == "pending"])
        cancelled = len([v for v in seller_visits if v.get("status") == "cancelled"])
        
        # Total de visitas programadas (completed + pending, sin cancelled)
        total_programmed = completed + pending
        
        # Calcular porcentaje de cumplimiento
        if total_programmed > 0:
            compliance_percentage = (completed / total_programmed) * 100
        else:
            compliance_percentage = 0.0
        
        # Obtener nombre de zona
        zone_name = next(
            (z["name"] for z in zones if z["id"] == seller.get("zone_id")),
            "Sin zona"
        )
        
        sellers_compliance.append(SellerComplianceItem(
            seller_id=seller["id"],
            seller_name=seller["name"],
            zone_id=seller.get("zone_id"),
            zone_name=zone_name,
            total_visits=total_programmed,
            completed_visits=completed,
            pending_visits=pending,
            cancelled_visits=cancelled,
            compliance_percentage=round(compliance_percentage, 2),
            period=PeriodInfo(
                start_date=start_date,
                end_date=end_date
            ) if start_date or end_date else None
        ))
        
        # Acumular para resumen
        total_visits_all += total_programmed
        total_completed_all += completed
        total_pending_all += pending
        total_cancelled_all += cancelled
        if total_programmed > 0:
            compliance_percentages.append(compliance_percentage)
    
    # Ordenar resultados
    reverse_order = sort_order == "desc"
    if sort_by == "compliance_percentage":
        sellers_compliance.sort(key=lambda x: x.compliance_percentage, reverse=reverse_order)
    elif sort_by == "seller_name":
        sellers_compliance.sort(key=lambda x: x.seller_name, reverse=reverse_order)
    elif sort_by == "total_visits":
        sellers_compliance.sort(key=lambda x: x.total_visits, reverse=reverse_order)
    
    # Calcular promedio de cumplimiento
    average_compliance = sum(compliance_percentages) / len(compliance_percentages) if compliance_percentages else 0.0
    
    # Crear resumen
    summary = ComplianceSummary(
        total_sellers=len(sellers_compliance),
        average_compliance=round(average_compliance, 2),
        total_visits=total_visits_all,
        total_completed=total_completed_all,
        total_pending=total_pending_all,
        total_cancelled=total_cancelled_all
    )
    
    # Crear período
    period = PeriodInfo(
        start_date=start_date,
        end_date=end_date
    )
    
    return VisitsComplianceResponse(
        sellers_compliance=sellers_compliance,
        summary=summary,
        period=period
    )


@router.get(
    "/sales-aggregated/sellers/{seller_id}",
    response_model=SellerAggregatedSalesResponse,
    summary="Reporte agregado de ventas por vendedor",
    description="Retorna el reporte agregado de ventas de todos los tenderos asignados a un vendedor específico."
)
async def get_seller_aggregated_sales(
    seller_id: int,
    start_date: date = Query(
        None,
        description="Fecha inicial del rango (YYYY-MM-DD). Por defecto últimos 30 días."
    ),
    end_date: date = Query(
        None,
        description="Fecha final del rango (YYYY-MM-DD)."
    ),
    current_user: dict = Depends(get_current_user),
    authorization: str = Header(None)
):
    """
    Reporte agregado de ventas de un vendedor
    
    Agrega las ventas de todos los tenderos asignados al vendedor
    para medir su rendimiento general.
    """
    token = authorization.replace("Bearer ", "") if authorization else None
    
    date_end = end_date or datetime.utcnow().date()
    date_start = start_date or (date_end - timedelta(days=30))
    
    if date_start > date_end:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail="La fecha inicial no puede ser posterior a la fecha final."
        )
    
    # Obtener información del vendedor
    seller = await ms_client.get_seller_by_id(seller_id, token=token)
    if not seller:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Vendedor no encontrado.")
    
    # Obtener zona del vendedor
    zone_name = None
    zone_id = seller.get("zone_id")
    if zone_id:
        zone = await ms_client.get_zone_by_id(zone_id)
        if zone:
            zone_name = zone.get("name")
    
    # Obtener todos los tenderos asignados al vendedor
    shopkeepers = await ms_client.get_all_shopkeepers(seller_id=seller_id, token=token)
    
    if not shopkeepers:
        # Si no hay tenderos asignados, retornar respuesta vacía
        return SellerAggregatedSalesResponse(
            report_generated_at=datetime.utcnow(),
            seller_id=seller_id,
            seller_name=seller.get("name"),
            seller_email=seller.get("email"),
            zone_name=zone_name,
            range_start=date_start,
            range_end=date_end,
            total_shopkeepers=0,
            summary=SalesSummary(
                total_records=0,
                total_units=0,
                total_amount=0.0,
                average_ticket=0.0
            ),
            shopkeepers_summary=[]
        )
    
    # Generar ventas para cada tendero y agregar
    shopkeepers_summary_list = []
    total_records = 0
    total_units = 0
    total_amount = 0.0
    
    for shopkeeper in shopkeepers:
        shopkeeper_id = shopkeeper.get("id")
        if not shopkeeper_id:
            continue
        
        # Generar ventas mock para este tendero
        rng = Random(shopkeeper_id)
        generated_sales = _generate_mock_sales(shopkeeper_id, rng)
        
        # Filtrar por rango de fechas
        filtered_sales = [
            sale for sale in generated_sales
            if date_start <= sale["sold_at"].date() <= date_end
        ]
        
        # Calcular resumen para este tendero
        shopkeeper_records = len(filtered_sales)
        shopkeeper_units = sum(sale["quantity"] for sale in filtered_sales)
        shopkeeper_amount = round(sum(sale["total_amount"] for sale in filtered_sales), 2)
        shopkeeper_avg_ticket = round(shopkeeper_amount / shopkeeper_records, 2) if shopkeeper_records else 0.0
        
        # Agregar al total
        total_records += shopkeeper_records
        total_units += shopkeeper_units
        total_amount += shopkeeper_amount
        
        # Agregar al resumen por tendero
        shopkeepers_summary_list.append(ShopkeeperSalesSummary(
            shopkeeper_id=shopkeeper_id,
            shopkeeper_name=shopkeeper.get("name"),
            shopkeeper_business_name=shopkeeper.get("business_name"),
            total_records=shopkeeper_records,
            total_units=shopkeeper_units,
            total_amount=shopkeeper_amount,
            average_ticket=shopkeeper_avg_ticket
        ))
    
    # Calcular ticket promedio total
    average_ticket = round(total_amount / total_records, 2) if total_records else 0.0
    
    return SellerAggregatedSalesResponse(
        report_generated_at=datetime.utcnow(),
        seller_id=seller_id,
        seller_name=seller.get("name"),
        seller_email=seller.get("email"),
        zone_name=zone_name,
        range_start=date_start,
        range_end=date_end,
        total_shopkeepers=len(shopkeepers),
        summary=SalesSummary(
            total_records=total_records,
            total_units=total_units,
            total_amount=round(total_amount, 2),
            average_ticket=average_ticket
        ),
        shopkeepers_summary=shopkeepers_summary_list
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
