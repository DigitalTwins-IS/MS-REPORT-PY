# MS-REPORT-PY

Microservicio de reportes y análisis para el sistema Digital Twins.

## 🚀 Quick Start

### Local Development
```bash
# Instalar dependencias
pip install -r requirements.txt

# Configurar variables de entorno
cp env.example .env

# Ejecutar servidor
uvicorn src.main:app --reload --host 0.0.0.0 --port 8000
```

### Docker
```bash
# Build
docker build -t ms-report-py .

# Run
docker run -p 8004:8000 --env-file .env ms-report-py
```

## 📋 Requirements

- Python 3.11+
- FastAPI, Pandas, OpenPyXL
- HTTP client para comunicación con otros microservicios

## 🔧 Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SECRET_KEY` | JWT secret key | Required |
| `MS_AUTH_URL` | Auth service URL | Required |
| `MS_GEO_URL` | Geo service URL | Required |
| `MS_USER_URL` | User service URL | Required |
| `MAX_EXPORT_RECORDS` | Max export records | 10000 |
| `REPORT_CACHE_TTL` | Cache TTL (seconds) | 300 |
| `SERVICE_PORT` | Service port | 8000 |

## 📡 Endpoints

- `GET /api/v1/reports/sellers` - Seller reports
- `GET /api/v1/reports/shopkeepers` - Shopkeeper reports
- `GET /api/v1/reports/assignments` - Assignment reports
- `GET /api/v1/reports/export/excel` - Export to Excel
- `GET /health` - Health check
- `GET /docs` - API documentation

## 🧪 Testing

```bash
pytest tests/ -v
```

## 📊 Features

- Data aggregation from multiple microservices
- Excel export functionality
- Report caching for performance
- Real-time data processing

