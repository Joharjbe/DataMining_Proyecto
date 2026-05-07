# Proyecto Spark — Setup

## Ubicación
/Users/nahiaescalante/Documents/UTEC/2026-1/Data_Mining/Proyecto/DataMining_Proyecto

## Entorno
- conda env: pySpark
- Python + PySpark + Jupyter

## Spark (cluster Docker)
- 1 master + 2 workers
- Memoria por worker: 3G
- Cores por worker: 2
- Total cluster: 6G + 4 cores
- Volumen montado: ruta del proyecto idéntica en host y contenedor (pyspark resuelve paths absolutos)

## Comandos para iniciar

```bash
cd /Users/nahiaescalante/Documents/UTEC/2026-1/Data_Mining/Proyecto/DataMining_Proyecto
conda activate pySpark
docker compose up -d
jupyter notebook
```

## Comandos para cerrar

```bash
docker compose down
conda deactivate
```

## URLs
- http://localhost:8080  → Spark Master UI (cluster status)
- http://localhost:8081  → Worker 1 UI
- http://localhost:8082  → Worker 2 UI
- http://localhost:4040  → Jobs UI (cuando corra un notebook conectado al cluster)

## Cómo conectar un notebook al cluster Docker

En lugar de `master("local[*]")`, usar:

```python
spark = (SparkSession.builder
    .appName("Fase2_Distribuido")
    .master("spark://localhost:7077")        # ← cluster Docker
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
```

⚠️ **Importante**: si `local[*]` y Docker corren a la vez, el puerto 4040 choca. Hacer `spark.stop()` antes de `docker compose up -d`, o cerrar el notebook con SparkSession local activo antes de conectar al cluster.
