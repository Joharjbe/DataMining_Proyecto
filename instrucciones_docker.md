# Proyecto Spark - Setup

## Ubicación
/Users/johar/Desktop/Data_Mining/proyecto

## Entorno
- venv: proyecto_dm
- Python + PySpark + Jupyter

## Spark
- Corre con Docker
- 1 master + 2 workers

## Comandos para iniciar

cd /Users/johar/Desktop/Data_Mining/proyecto
source proyecto_dm/bin/activate
docker compose up -d
jupyter notebook

## Comandos para cerrar

docker compose down
deactivate

## URLs
- http://localhost:8080 (Spark UI)
- http://localhost:4040 (Jobs)
