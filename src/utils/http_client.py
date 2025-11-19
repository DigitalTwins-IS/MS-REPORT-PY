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
                params["start_date"] = start_date.isoformat()
            if end_date:
                params["end_date"] = end_date.isoformat()
            if status_filter:
                params["status_filter"] = status_filter
            
            # Obtener todas las visitas (sin límite para el reporte)
            params["limit"] = 1000
            params["skip"] = 0
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    f"{self.ms_user_url}/api/v1/users/visits",
                    params=params,
                    headers=headers
                )
                
                if response.status_code == 200:
                    data = response.json()
                    # Si la respuesta tiene la estructura VisitListResponse
                    if isinstance(data, dict) and "visits" in data:
                        return data["visits"]
                    # Si es una lista directa
                    elif isinstance(data, list):
                        return data
                    return []
                else:
                    # Log del error para debugging
                    print(f"Error obteniendo visitas: {response.status_code} - {response.text}")
                    return []
                
        except httpx.RequestError as e:
            print(f"Error de conexión obteniendo visitas: {str(e)}")
            return []
        except Exception as e:
            print(f"Error inesperado obteniendo visitas: {str(e)}")
            return []


# Instancia global del cliente
ms_client = MicroserviceClient()

