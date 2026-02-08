@echo off
REM Script de démarrage rapide pour Windows

echo ========================================
echo  E-Commerce Analytics - Quick Start
echo ========================================
echo.

REM Vérifier Docker
echo [1/5] Checking Docker...
docker --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Docker not found! Please install Docker Desktop.
    pause
    exit /b 1
)
echo OK - Docker is installed

REM Vérifier Docker Compose
echo [2/5] Checking Docker Compose...
docker-compose --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Docker Compose not found!
    pause
    exit /b 1
)
echo OK - Docker Compose is installed

REM Démarrer les services
echo.
echo [3/5] Starting Docker services...
echo This may take a few minutes on first run...
docker-compose up -d

if errorlevel 1 (
    echo ERROR: Failed to start services
    pause
    exit /b 1
)

REM Attendre que les services soient prêts
echo.
echo [4/5] Waiting for services to be ready...
timeout /t 30 /nobreak > nul

REM Vérifier l'état
echo.
echo [5/5] Checking services status...
docker-compose ps

echo.
echo ========================================
echo  Services Started Successfully!
echo ========================================
echo.
echo Web Interfaces:
echo   - Neo4j Browser:    http://localhost:7474
echo   - Mongo Express:    http://localhost:8091
echo   - Kafka UI:         http://localhost:8090
echo   - RedisInsight:     http://localhost:8001
echo   - Flink Dashboard:  http://localhost:8081
echo   - Spark:            http://localhost:8082
echo   - Airflow:          http://localhost:8080
echo   - Superset:         http://localhost:8088
echo   - MinIO Console:    http://localhost:9001
echo.
echo Next Steps:
echo   1. Configure Kaggle API: Place kaggle.json in %%USERPROFILE%%\.kaggle\
echo   2. Download dataset: python scripts\download_dataset.py
echo   3. Load data: python ingestion\load_olist_dataset.py
echo   4. Launch dashboard: streamlit run visualization\streamlit_app.py
echo.
echo To stop services: docker-compose down
echo ========================================
pause
