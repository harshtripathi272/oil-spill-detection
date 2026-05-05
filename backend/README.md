# Oil Spill Detection Backend

A comprehensive FastAPI backend for the Oil Spill Detection system, providing REST APIs for dashboards, analytics, and system monitoring.

## Features

### Dashboard APIs
- **Real-time Statistics**: Total incidents, active incidents, processing metrics
- **Interactive Charts**: Time series, pie charts, bar charts for various metrics
- **Geographic Data**: Incident locations and regional distributions
- **Performance Metrics**: Model confidence scores, processing times

### Incident Management
- **CRUD Operations**: Create, read, update incidents
- **Status Tracking**: Monitor incident lifecycle (detected → confirmed → resolved)
- **Geographic Queries**: Filter incidents by location and region
- **Timeline Views**: Historical incident data

### System Monitoring
- **Health Checks**: Database, disk, memory, CPU monitoring
- **Airflow Integration**: DAG status, task monitoring
- **Resource Usage**: System resource consumption tracking
- **Process Monitoring**: Running processes and their resource usage

### Metrics & Analytics
- **Time Series Data**: Historical metrics with configurable time ranges
- **Performance Analytics**: Model accuracy, processing efficiency
- **Custom Metrics**: Extensible metric collection system
- **Alerting Support**: Threshold-based monitoring

## API Endpoints

### Dashboard
```
GET /api/v1/dashboard/stats - Overall statistics
GET /api/v1/dashboard/overview - Complete dashboard data
GET /api/v1/dashboard/charts/incidents-over-time - Incidents timeline
GET /api/v1/dashboard/charts/processing-times - Processing performance
GET /api/v1/dashboard/charts/status-distribution - Status breakdown
GET /api/v1/dashboard/charts/model-performance - Model metrics
GET /api/v1/dashboard/recent-incidents - Latest incidents
```

### Incidents
```
GET /api/v1/incidents/ - List incidents with filtering
GET /api/v1/incidents/{id} - Get specific incident
PUT /api/v1/incidents/{id}/status - Update incident status
GET /api/v1/incidents/{id}/dag-runs - Get related DAG runs
GET /api/v1/incidents/stats/status-breakdown - Status statistics
GET /api/v1/incidents/stats/geographic-distribution - Geographic stats
GET /api/v1/incidents/timeline - Timeline data
```

### Metrics
```
GET /api/v1/metrics/ - List metrics with filtering
GET /api/v1/metrics/categories - Available categories
GET /api/v1/metrics/time-series/{name} - Time series data
GET /api/v1/metrics/system/health - System health metrics
GET /api/v1/metrics/model/performance - Model performance
GET /api/v1/metrics/processing/stats - Processing statistics
POST /api/v1/metrics/ - Create new metric
```

### System
```
GET /api/v1/system/health - Overall system health
GET /api/v1/system/resources - Resource usage details
GET /api/v1/system/processes - Running processes
GET /api/v1/system/airflow/status - Airflow status
GET /api/v1/system/logs/recent - Recent system logs
GET /api/v1/system/config - System configuration
```

## Installation

### Local Development

1. **Clone and setup**:
```bash
cd backend
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. **Install dependencies**:
```bash
pip install -r requirements.txt
```

3. **Configure environment**:
```bash
cp .env.example .env
# Edit .env with your database and other settings
```

4. **Run database migrations**:
```bash
alembic upgrade head
```

5. **Start the server**:
```bash
uvicorn app.main:app --reload
```

### Docker

```bash
docker build -t oil-spill-backend .
docker run -p 8000:8000 oil-spill-backend
```

## Configuration

### Environment Variables

```bash
# Database
DATABASE_URL=postgresql://user:password@localhost:5432/oilspill_db

# Airflow Integration
AIRFLOW_DB_URL=sqlite:////data/user13/airflow/airflow.db
AIRFLOW_DAGS_FOLDER=/data/user13/oilspill_ugq/oil-spill-detection/orchestration/dags

# API Settings
API_HOST=0.0.0.0
API_PORT=8000
DEBUG=true

# Security
SECRET_KEY=your-secret-key-here
ACCESS_TOKEN_EXPIRE_MINUTES=30

# External Services
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
SAR_TRIGGER_TOPIC=sar-trigger-events
```

## Database Schema

### Core Tables

- **incidents**: Oil spill detections with location, confidence, status
- **dag_runs**: Airflow DAG execution records
- **task_instances**: Individual task execution details
- **metrics**: System and model performance metrics
- **system_status**: Component health monitoring

## Data Flow

1. **Incident Detection**: SAR images processed, incidents created
2. **DAG Execution**: Airflow orchestrates download and inference
3. **Metrics Collection**: Performance data stored in database
4. **API Serving**: Dashboard queries processed and served
5. **Real-time Updates**: WebSocket support for live updates

## Monitoring & Alerting

- **Health Checks**: Automated system health monitoring
- **Threshold Alerts**: Configurable alerts for metrics
- **Performance Tracking**: Historical performance analysis
- **Error Logging**: Comprehensive error tracking and reporting

## Security

- **API Authentication**: JWT-based authentication
- **CORS Configuration**: Configurable cross-origin policies
- **Input Validation**: Pydantic models for data validation
- **Rate Limiting**: Configurable request rate limits

## Development

### Running Tests

```bash
pytest
```

### Code Formatting

```bash
black .
isort .
```

### API Documentation

Access the interactive API documentation at `http://localhost:8000/docs`

## Deployment

### Production Considerations

- **Database**: Use PostgreSQL in production
- **Caching**: Implement Redis for session and data caching
- **Load Balancing**: Use nginx or similar for load distribution
- **Monitoring**: Integrate with Prometheus/Grafana for metrics
- **Logging**: Centralized logging with ELK stack

### Docker Compose Example

```yaml
version: '3.8'
services:
  backend:
    build: .
    ports:
      - "8000:8000"
    environment:
      - DATABASE_URL=postgresql://user:password@db:5432/oilspill_db
    depends_on:
      - db

  db:
    image: postgres:13
    environment:
      - POSTGRES_DB=oilspill_db
      - POSTGRES_USER=user
      - POSTGRES_PASSWORD=password
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the MIT License.