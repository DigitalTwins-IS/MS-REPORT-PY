"""
Router de Reportes y Análisis
"""
from fastapi import APIRouter, Depends, HTTPException, status, Query, Header
from fastapi.responses import StreamingResponse
from typing import Optional, List
from datetime import datetime, timedelta, date
import io
import csv
import json
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
    TopProductItem,
    TopProductsResponse,
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

