"""
MS-REPORT-PY - Microservicio de Reportes y Análisis
FastAPI Application
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse

from .config import settings

# Crear aplicación FastAPI
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="""
    Microservicio de reportes y análisis para el Sistema Digital Twins.
    
    ## Funcionalidades
    
    * **Reportes de Cobertura**: Análisis territorial por zonas y ciudades
    * **Rendimiento de Vendedores**: Métricas de eficiencia y carga de trabajo
    * **Estadísticas por Zona**: Datos detallados de cada zona
    * **Métricas del Sistema**: Dashboard con indicadores clave
    * **Exportación**: CSV, JSON para análisis externo
    
    ## Integraciones
    
    Este microservicio consume datos de:
    * **MS-AUTH-PY**: Autenticación
    * **MS-GEO-PY**: Datos geográficos
    * **MS-USER-PY**: Vendedores, tenderos y asignaciones
    """,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json"
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Importar routers
from .routers import reports_router

# Incluir routers
app.include_router(
    reports_router,
    prefix=settings.API_PREFIX,
    tags=["reports"]
)


@app.get("/", include_in_schema=False)
async def root():
    """Redireccionar a la documentación"""
    return RedirectResponse(url="/docs")


@app.get("/health", tags=["Health"])
async def root_health():
    """Health check raíz"""
    return {
        "status": "healthy",
        "service": settings.APP_NAME,
        "version": settings.APP_VERSION
    }


# Event handlers
@app.on_event("startup")
async def startup_event():
    """Evento de inicio de la aplicación"""
    print(f"📊 {settings.APP_NAME} v{settings.APP_VERSION} iniciado")
    print(f"📚 Documentación disponible en: http://{settings.SERVICE_HOST}:{settings.SERVICE_PORT}/docs")
    print(f"📈 Endpoints de reportes en: {settings.API_PREFIX}")
    print(f"🔗 Integraciones:")
    print(f"   - MS-GEO:  {settings.MS_GEO_URL}")
    print(f"   - MS-USER: {settings.MS_USER_URL}")
    print(f"   - MS-AUTH: {settings.MS_AUTH_URL}")


@app.on_event("shutdown")
async def shutdown_event():
    """Evento de cierre de la aplicación"""
    print(f"🛑 {settings.APP_NAME} detenido")


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "main:app",
        host=settings.SERVICE_HOST,
        port=settings.SERVICE_PORT,
        reload=settings.DEBUG
    )

