"""
Cliente HTTP para comunicarse con otros microservicios
"""
import httpx
from typing import Optional, List
from datetime import datetime
from fastapi import HTTPException, status
from ..config import settings


class MicroserviceClient:
    """Cliente para interactuar con otros microservicios"""
    
    def __init__(self):
        self.timeout = 10.0
        self.ms_geo_url = settings.MS_GEO_URL
        self.ms_user_url = settings.MS_USER_URL
        self.ms_auth_url = settings.MS_AUTH_URL
    
    async def get_all_cities(self) -> List[dict]:
        """Obtiene todas las ciudades desde MS-GEO-PY"""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(f"{self.ms_geo_url}/api/v1/geo/cities")
                
                if response.status_code == 200:
                    return response.json()
                return []
                
        except httpx.RequestError:
            return []
    
    async def get_all_zones(self, city_id: Optional[int] = None) -> List[dict]:
        """Obtiene todas las zonas desde MS-GEO-PY"""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                params = {"city_id": city_id} if city_id else {}
                response = await client.get(
                    f"{self.ms_geo_url}/api/v1/geo/zones",
                    params=params
                )
                
                if response.status_code == 200:
                    return response.json()
                return []
                
        except httpx.RequestError:
            return []
    
    async def get_zone_by_id(self, zone_id: int) -> Optional[dict]:
        """Obtiene una zona específica"""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(f"{self.ms_geo_url}/api/v1/geo/zones/{zone_id}")
                
                if response.status_code == 200:
                    return response.json()
                return None
                
        except httpx.RequestError:
            return None
    
    async def get_all_sellers(self, zone_id: Optional[int] = None, token: str = None) -> List[dict]:
        """Obtiene todos los vendedores desde MS-USER-PY"""
        try:
            headers = {"Authorization": f"Bearer {token}"} if token else {}
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                params = {"zone_id": zone_id} if zone_id else {}
                response = await client.get(
                    f"{self.ms_user_url}/api/v1/users/sellers",
                    params=params,
                    headers=headers
                )
                
                if response.status_code == 200:
                    return response.json()
                return []
                
        except httpx.RequestError:
            return []
    
    async def get_all_shopkeepers(
        self, 
        seller_id: Optional[int] = None,
        unassigned: bool = False,
        token: str = None
    ) -> List[dict]:
        """Obtiene todos los tenderos desde MS-USER-PY"""
        try:
            headers = {"Authorization": f"Bearer {token}"} if token else {}
            params = {}
            if seller_id:
                params["seller_id"] = seller_id
            if unassigned:
                params["unassigned"] = "true"
                
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    f"{self.ms_user_url}/api/v1/users/shopkeepers",
                    params=params,
                    headers=headers
                )
                
                if response.status_code == 200:
                    return response.json()
                return []
                
        except httpx.RequestError:
            return []
    
    async def get_all_assignments(self, seller_id: Optional[int] = None, token: str = None) -> List[dict]:
        """Obtiene todas las asignaciones desde MS-USER-PY"""
        try:
            headers = {"Authorization": f"Bearer {token}"} if token else {}
            params = {"seller_id": seller_id} if seller_id else {}
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    f"{self.ms_user_url}/api/v1/users/assignments",
                    params=params,
                    headers=headers
                )
                
                if response.status_code == 200:
                    return response.json()
                return []
                
        except httpx.RequestError:
            return []
    
    async def get_shopkeeper_by_id(self, shopkeeper_id: int, token: str = None) -> Optional[dict]:
        """Obtiene el detalle de un tendero específico"""
        try:
            headers = {"Authorization": f"Bearer {token}"} if token else {}
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    f"{self.ms_user_url}/api/v1/users/shopkeepers/{shopkeeper_id}",
                    headers=headers
                )
                if response.status_code == 200:
                    return response.json()
                return None
        except httpx.RequestError:
            return None
    
    async def get_seller_by_id(self, seller_id: int, token: str = None) -> Optional[dict]:
        """Obtiene el detalle de un vendedor específico"""
        try:
            headers = {"Authorization": f"Bearer {token}"} if token else {}
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    f"{self.ms_user_url}/api/v1/users/sellers/{seller_id}",
                    headers=headers
                )
                if response.status_code == 200:
                    return response.json()
                return None
        except httpx.RequestError:
            return None
    
    async def get_inventory_by_shopkeeper(self, shopkeeper_id: int, token: str = None) -> List[dict]:
        """Obtiene el inventario completo de un tendero"""
        try:
            headers = {"Authorization": f"Bearer {token}"} if token else {}
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    f"{self.ms_user_url}/api/v1/users/inventory/{shopkeeper_id}",
                    headers=headers
                )
                if response.status_code == 200:
                    return response.json()
                return []
        except httpx.RequestError:
            return []
    
    async def check_service_health(self, service_url: str) -> str:
        """Verifica el estado de un microservicio"""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"{service_url}/health")
                
                if response.status_code == 200:
                    return "connected"
                return "unhealthy"
                
        except httpx.RequestError:
            return "disconnected"
    
    async def get_visits(
        self,
        seller_id: Optional[int] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        status_filter: Optional[str] = None,
        token: str = None
    ) -> List[dict]:
        """Obtiene visitas desde MS-USER-PY"""
        try:
            headers = {"Authorization": f"Bearer {token}"} if token else {}
            params = {}
            
            if seller_id:
                params["seller_id"] = seller_id
            if start_date:
                # Asegurar que la fecha tenga formato ISO con timezone
                if isinstance(start_date, datetime):
                    params["start_date"] = start_date.isoformat()
                else:
                    params["start_date"] = str(start_date)
            if end_date:
                # Asegurar que la fecha tenga formato ISO con timezone
                if isinstance(end_date, datetime):
                    params["end_date"] = end_date.isoformat()
                else:
                    params["end_date"] = str(end_date)
            if status_filter:
                params["status_filter"] = status_filter
            
            # Obtener todas las visitas con paginación si es necesario
            all_visits = []
            skip = 0
            limit = 1000
            max_iterations = 100  # Prevenir loops infinitos
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                for iteration in range(max_iterations):
                    params_page = params.copy()
                    params_page["limit"] = limit
                    params_page["skip"] = skip
                    
                    response = await client.get(
                        f"{self.ms_user_url}/api/v1/users/visits",
                        params=params_page,
                        headers=headers
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        # Si la respuesta tiene la estructura VisitListResponse
                        if isinstance(data, dict) and "visits" in data:
                            visits_batch = data["visits"]
                            total = data.get("total", len(visits_batch))
                            all_visits.extend(visits_batch)
                            
                            # Si obtuvimos menos visitas que el límite, ya terminamos
                            if len(visits_batch) < limit or len(all_visits) >= total:
                                break
                            
                            skip += limit
                        # Si es una lista directa
                        elif isinstance(data, list):
                            all_visits.extend(data)
                            # Si obtuvimos menos que el límite, ya terminamos
                            if len(data) < limit:
                                break
                            skip += limit
                        else:
                            break
                    else:
                        # Log del error para debugging
                        print(f"Error obteniendo visitas: {response.status_code} - {response.text}")
                        break
                
                # Log para debugging
                print(f"Obtenidas {len(all_visits)} visitas totales para seller_id={seller_id}")
                return all_visits
                
        except httpx.RequestError as e:
            print(f"Error de conexión obteniendo visitas: {str(e)}")
            return []
        except Exception as e:
            print(f"Error inesperado obteniendo visitas: {str(e)}")
            return []


# Instancia global del cliente
ms_client = MicroserviceClient()

