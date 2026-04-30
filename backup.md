# Proyecto 1 — Procesamiento y Análisis de Datos Masivos
**Data Mining · UTEC · Prof. Heider Sanchez**
Dataset: MovieLens 20M · 20M ratings · 138K usuarios · 27K películas

---

## Estructura de carpetas

```
proyecto/
│
├── dataset/                          ← no se modifica
│   ├── ratings.csv                   (533 MB — 20M calificaciones)
│   ├── movies.csv                    (1.4 MB — 27K películas)
│   ├── tags.csv                      (16.6 MB — tags de usuarios)
│   └── genome-scores.csv             (323 MB)
│
├── notebooks/                        ← desarrollo principal, uno por fase
│   ├── fase1_preprocesamiento_eda.ipynb
│   ├── fase2_procesamiento_distribuido.ipynb
│   ├── fase3_minhash_lsh.ipynb
│   └── fase4_reglas_asociacion.ipynb
│
├── src/                              ← módulos con la lógica reutilizable
│   ├── spark_session.ipynb           (configurar SparkSession local / cluster)
│   ├── preprocesamiento.ipynb        (funciones de la Fase 1)
│   ├── proc_distribuido.ipynb        (funciones de la Fase 2)
│   ├── minhash_lsh.ipynb             (funciones de la Fase 3)
│   └── reglas_asociacion.ipynb       (funciones de la Fase 4)
│
├── outputs/
│   ├── figures/                      ← gráficos PNG generados por los notebooks
│   └── results/                      ← archivos Parquet y CSV intermedios
│
├── proyecto_dm/                      ← entorno virtual Python (no se modifica)
├── docker-compose.yml                ← cluster Spark: 1 master + 2 workers
├── instrucciones_docker.md
├── arquitectura_proyecto.excalidraw  ← diagrama de arquitectura (abrir en VSCode)
└── README.md
```

> **Nota sobre src/:** los módulos se desarrollan en `.ipynb` para poder ver Spark en acción.
> Al finalizar el proyecto se convierten a `.py` para cumplir el entregable de código modular.

---

## Cómo ejecutar

### 1. Activar el entorno virtual
```bash
cd /Users/johar/Desktop/Data_Mining/proyecto
source proyecto_dm/bin/activate
```

### 2. Levantar el cluster Spark (Docker)
```bash
docker compose up -d
# Verificar: http://localhost:8080
```

### 3. Abrir Jupyter
```bash
cd notebooks/
jupyter notebook
```

### 4. Orden de ejecución
Los notebooks deben ejecutarse en orden. El de Fase 1 genera los Parquet que usan los demás.

---


## Dataset — contexto

MovieLens es una plataforma de recomendación de películas de la Universidad de Minnesota (GroupLens Research). Este dataset es una exportación de toda la actividad de la plataforma entre 1995 y 2015: 138,493 usuarios que calificaron al menos 20 películas cada uno, sobre un catálogo de 27,278 películas, generando 20 millones de ratings en escala de 0.5 a 5.0 estrellas. Es un caso clásico de sistema de recomendación con suficiente volumen para evidenciar los retos de escalabilidad de cada técnica.

## Dataset — archivos y uso

| Archivo | Contenido | Uso |
|---|---|---|
| `ratings.csv` | 20M ratings: userId, movieId, rating, timestamp | ✅ Todas las fases |
| `movies.csv` | 27K películas: movieId, title, genres | ✅ Todas las fases |
| `tags.csv` | 465K tags de usuarios: userId, movieId, tag, timestamp | ✅ EDA + Fase 3 |
| `links.csv` | IDs de películas en IMDB y TMDB | ❌ No se usa |
| `genome-scores.csv` | Matriz densa película-tag generada por ML (323MB) | ❌ No se usa |
| `genome-tags.csv` | Nombres de tags del genome | ❌ No se usa |

**Esquema de relaciones:**
```
ratings (userId, movieId ─┐, rating, timestamp)
tags    (userId, movieId ─┤, tag,    timestamp)
                          ↓
movies              (movieId, title, genres)
```
Los 3 archivos se unen por `movieId`. `userId` es compartido entre `ratings` y `tags` pero no existe tabla separada de usuarios.

**Usamos:**
- `ratings.csv` es el corazón: de aquí sale el EDA, el ranking, la binarización like/dislike, la representación para Jaccard y las transacciones para FP-Growth.
- `movies.csv` hace los resultados legibles: títulos y géneros en vez de solo IDs.
- `tags.csv` aporta al EDA y es la representación alternativa de películas en la Fase 3 (Opción B: película = conjunto de tags).

**Descartamos:**
- `links.csv` solo cruza IDs con IMDB/TMDB, no aporta nada a nuestro análisis.
- `genome-scores.csv` es una matriz ML de relevancia película-tag (~10M filas, 323MB). El proyecto no la requiere.
- `genome-tags.csv` solo tiene sentido junto a genome-scores, cae por descarte.

---

## Progreso por fases

### ✅ Fase 0 — Entorno (completada)
- PySpark 4.1.1 en venv `proyecto_dm`
- SparkSession local: lee dataset, UI en `localhost:4040`
- Cluster Docker: 1 master + 2 workers · 20 cores · 13.3 GiB
- `docker-compose.yml` ajustado: solo puertos 8080 y 7077

> ⚠️ No correr SparkSession local y Docker al mismo tiempo — el puerto 4040 choca. Hacer `spark.stop()` antes de `docker compose up -d`.

---

## Rúbrica (20 puntos)

| Criterio | Descripción | Pts. |
|---|---|---|
| Preprocesamiento | Limpieza, binarización y unificación de géneros | 5 |
| Implementación técnica | Spark, MinHash, LSH y reglas de asociación | 6 |
| Análisis y resultados | Gráficos EDA y curvas de desempeño LSH | 4 |
| Discusión crítica | Escalabilidad, FP/FN, aplicabilidad real | 3 |
| Código y reproducibilidad | Código comentado + README ejecutable | 2 |

---

## Stack técnico

- Python 3.11 · PySpark · Jupyter
- Apache Spark (Docker: 1 master + 2 workers)
- matplotlib · pandas · numpy
- Spark MLlib (MinHashLSH, FP-Growth)
