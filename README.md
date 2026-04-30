# Proyecto 1 — Procesamiento y Análisis de Datos Masivos

**Curso:** Data Mining · UTEC · **Profesor:** Heider Sanchez
**Dataset:** MovieLens 20M · 20,000,263 ratings · 138,493 usuarios · 27,278 películas
**Stack:** Python 3.11 · PySpark 4.1.1 · Apache Spark (Docker) · Spark MLlib · Jupyter

---

## Tabla de contenido

1. [Resumen del proyecto](#resumen-del-proyecto)
2. [Estructura de carpetas](#estructura-de-carpetas)
3. [Cómo ejecutar](#cómo-ejecutar)
4. [Dataset](#dataset)
5. [Metodología por fase](#metodología-por-fase)
6. [Resultados clave](#resultados-clave)
7. [Progreso](#progreso)
8. [Stack técnico](#stack-técnico)
9. [Rúbrica](#rúbrica-20-puntos)
10. [Referencias](#referencias)

---

## Resumen del proyecto

El proyecto aplica técnicas de Data Mining sobre **MovieLens 20M** para resolver tres problemas a escala distribuida con PySpark:

1. **Caracterizar el dataset** mediante EDA (distribuciones, sparsity, evolución temporal, géneros, tags).
2. **Procesamiento distribuido**: comparar RDDs vs DataFrames y obtener el top-20 de películas más calificadas.
3. **Búsqueda de pares similares** con Jaccard exacto, MinHashing y Locality-Sensitive Hashing (LSH), evaluando precisión y recall del modelo aproximado.
4. **Reglas de asociación** con FP-Growth para encontrar patrones de co-consumo de películas (Fase 4 — pendiente).

Los entregables son: (i) código modular reproducible y (ii) un reporte técnico de máximo 8 páginas con gráficos y discusión crítica.

---

## Estructura de carpetas

```
proyecto/
│
├── dataset/                          ← MovieLens 20M (no se modifica)
│   ├── ratings.csv                   533 MB · 20M calificaciones
│   ├── movies.csv                    1.4 MB · 27K películas
│   ├── tags.csv                      16.6 MB · 465K tags
│   ├── links.csv                     no se usa
│   ├── genome-scores.csv             no se usa (323 MB)
│   └── genome-tags.csv               no se usa
│
├── notebooks/                        ← desarrollo principal, uno por fase
│   ├── fase0_verificacion.ipynb      verificación entorno y cluster
│   ├── fase1_preprocesamiento_eda.ipynb
│   ├── fase2_procesamiento_distribuido.ipynb
│   ├── fase3_minhash_lsh.ipynb
│   └── fase4_reglas_asociacion.ipynb
│
├── src/                              ← módulos reutilizables (.py)
│
├── outputs/
│   ├── figures/                      ← 13 PNG generados por los notebooks
│   ├── results/
│   │   ├── ratings_clean.parquet     20M filas + columna like
│   │   ├── movies_clean.parquet      27K películas con géneros unificados
│   │   └── tags_clean.parquet        463K tags filtrados
│   ├── Reporte_Proyecto1.pdf         ← reporte técnico final
│   └── Diapositivas_Proyecto1.pptx   ← slides para sustentación
│
├── proyecto_dm/                      ← virtualenv Python (no se modifica)
├── docker-compose.yml                ← cluster Spark: 1 master + 2 workers
├── instrucciones_docker.md
├── arquitectura_proyecto.excalidraw  ← diagrama de arquitectura
├── Fases.md                          ← plan de desarrollo
├── Requisitos_Proyecto_1.pdf         ← enunciado del curso
└── README.md
```

> Los notebooks de `src/` se desarrollan en `.ipynb` para visualizar Spark y se exportan a `.py` al final del proyecto para cumplir con el entregable de **código modular**.

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
# Verificar Master: http://localhost:8080
# Verificar Jobs:  http://localhost:4040 (cuando corra un notebook)
```

### 3. Abrir Jupyter
```bash
cd notebooks/
jupyter notebook
```

### 4. Orden de ejecución
Los notebooks **deben ejecutarse en orden**. La Fase 1 produce los Parquet limpios que las demás fases consumen.

| # | Notebook | Salida que produce |
|---|---|---|
| 0 | `fase0_verificacion.ipynb` | Verifica PySpark + cluster Docker |
| 1 | `fase1_preprocesamiento_eda.ipynb` | Parquets en `outputs/results/` + 7 figuras EDA |
| 2 | `fase2_procesamiento_distribuido.ipynb` | Top-20 + benchmark RDD vs DataFrame |
| 3 | `fase3_minhash_lsh.ipynb` | Curvas LSH precisión-recall |
| 4 | `fase4_reglas_asociacion.ipynb` | (pendiente) Reglas FP-Growth |

> ⚠️ **No correr SparkSession local y Docker al mismo tiempo** — el puerto 4040 choca. Hacer `spark.stop()` antes de `docker compose up -d`.

---

## Dataset

MovieLens es la plataforma de recomendación de películas de la Universidad de Minnesota (GroupLens Research). El dataset es una exportación de toda la actividad entre 1995 y 2015: **138,493 usuarios** que calificaron al menos 20 películas cada uno, sobre un catálogo de **27,278 películas**, generando **20 millones de ratings** en escala de 0.5 a 5.0 estrellas. Es un caso clásico de sistema de recomendación con suficiente volumen para evidenciar los retos de escalabilidad de cada técnica.

### Archivos y uso

| Archivo | Contenido | Uso |
|---|---|---|
| `ratings.csv` | 20M ratings: userId, movieId, rating, timestamp | ✅ Todas las fases |
| `movies.csv` | 27K películas: movieId, title, genres | ✅ Todas las fases |
| `tags.csv` | 465K tags: userId, movieId, tag, timestamp | ✅ EDA + Fase 3 |
| `links.csv` | IDs de IMDB y TMDB | ❌ No se usa |
| `genome-scores.csv` | Matriz densa película-tag (323 MB) | ❌ No se usa |
| `genome-tags.csv` | Nombres de tags del genome | ❌ No se usa |

### Esquema de relaciones

```
ratings (userId, movieId ─┐, rating, timestamp)
tags    (userId, movieId ─┤, tag,    timestamp)
                          ↓
movies              (movieId, title, genres)
```

`movieId` une los tres archivos. `userId` es compartido entre `ratings` y `tags` pero no existe tabla separada de usuarios.

---

## Metodología por fase

### Fase 1 · Preprocesamiento y EDA

**Limpieza:**
- Sin nulos, sin duplicados, sin ratings fuera de rango (escala 0.5–5.0).
- 246 películas sin género → marcadas como `Unknown` (1 película extra detectada con campo vacío luego del split, total 247).
- 2,334 tags técnicos `bd-r` (Blu-ray Disc) eliminados.
- Géneros unificados: `Sci-Fi → Science Fiction`, `Film-Noir → Film Noir`, `Children → Children's`. `IMAX` eliminado por no ser un género real (es un formato).

**Binarización like/dislike:**
- Umbral: `rating ≥ 3.5 → like (1)`, `rating < 3.5 → dislike (0)`.
- Justificación: 3.5 es la media del dataset (3.53) y el punto donde el usuario conscientemente da media estrella extra.
- Resultado: **61% likes, 39% dislikes**.

**Persistencia:** los tres datasets limpios se guardan en formato **Parquet** en `outputs/results/` para reutilizar en las fases siguientes.

**EDA realizado:** distribución de ratings, ratings por usuario, ratings por película, distribución de géneros, sparsity de la matriz, evolución temporal y análisis de tags.

### Fase 2 · Procesamiento distribuido

- Conteo de ratings por película con **DataFrame API** (`groupBy + count + orderBy`).
- Mismo conteo con **RDD estilo MapReduce** (`map + reduceByKey + sortBy`).
- Top-20 con `join` a `movies_clean` para obtener títulos legibles.
- **Benchmark** de tres operaciones (conteo por película, top-20 con join, conteo por usuario) midiendo tiempos de RDD vs DataFrame.

### Fase 3 · Similitud, MinHash y LSH

**Representación elegida — Opción A:** cada película es el conjunto de usuarios que le dieron `like` (rating ≥ 3.5). Justificación: usuarios y likes están directamente en el dataset masivo; se aprovechan los 20M de ratings.

**Filtro de calidad:** películas con menos de 50 likes se descartan. El análisis de sensibilidad confirma que con tamaños ≥ 50 la similitud Jaccard es estable (cambio < 1.5% al agregar un usuario). Quedan **8,362 películas** con firma.

**Pipeline:**
1. **Jaccard exacto** sobre muestra de 200 películas (19,900 pares) como ground truth.
2. **MinHash manual con RDD**: 100 funciones hash de la forma `h(x) = (a·x + b) mod p mod N` con `p = 2,147,483,647` (Mersenne).
3. **MLlib `MinHashLSH`**: implementación de referencia para comparación.
4. **LSH banding manual**: variar `b` (bandas) y `r` (filas) con `b × r = 100` constante.
5. **Métricas**: precisión, recall, MAE de Jaccard estimado vs exacto, correlación de Pearson.

### Fase 4 · Reglas de asociación (pendiente)

Construcción de transacciones `usuario → lista de películas con like`, ejecución de **FP-Growth** (Spark MLlib), extracción de itemsets frecuentes y reglas con soporte/confianza/lift. Esta fase aún no está implementada.

---

## Resultados clave

### Hallazgos del EDA

| # | Hallazgo | Evidencia |
|---|----------|-----------|
| 1 | Distribución sesgada hacia ratings positivos: media = 3.53, mediana = 3.5 | `01_distribucion_ratings.png` |
| 2 | Ratings por usuario siguen power-law: P75 = 155, máx = 9,254 | `02_ratings_por_usuario.png` |
| 3 | Long-tail de películas: P50 = 18 ratings, P75 = 205, máx = 67,310 (Pulp Fiction) | `03_ratings_por_pelicula.png` |
| 4 | Drama (13,344) y Comedy (8,374) dominan más del 50% del catálogo | `04_distribucion_generos.png` |
| 5 | Sparsity = **99.46%** (3.7B celdas posibles, solo 20M con dato) | `05_sparsity.png` |
| 6 | Picos de actividad en 2000 y 2005 (auge del DVD y plataformas web) | `06_evolucion_temporal.png` |
| 7 | Tag dominante: `sci-fi` con 3,576 usos | `07_top_tags.png` |

### Top-20 películas más calificadas (Fase 2)

| # | Película | # ratings |
|---|---|---|
| 1 | Pulp Fiction (1994) | 67,310 |
| 2 | Forrest Gump (1994) | 66,172 |
| 3 | The Shawshank Redemption (1994) | 63,366 |
| 4 | The Silence of the Lambs (1991) | 63,299 |
| 5 | Jurassic Park (1993) | 59,715 |
| 6 | Star Wars Episode IV (1977) | 54,502 |
| 7 | Braveheart (1995) | 53,769 |
| 8 | Terminator 2 (1991) | 52,244 |
| 9 | The Matrix (1999) | 51,334 |
| 10 | Schindler's List (1993) | 50,054 |

### RDD vs DataFrame (Fase 2)

DataFrame es **consistentemente más rápido** que RDD gracias al optimizador **Catalyst** y a Tungsten. La aceleración varía según la complejidad de la operación:

| Operación | Speedup DataFrame |
|---|---|
| Conteo por película | ~119× |
| Top-20 con join | ~10× |
| Conteo por usuario | ~14× |

> El 119× del primer caso está inflado por caché en memoria; los otros (10×–14×) son representativos de un escenario realista.

### MinHash vs Jaccard exacto (Fase 3)

Sobre 19,900 pares evaluados:

| Métrica | Valor |
|---|---|
| Error promedio (MAE) | 0.0104 |
| Error mediano | 0.0066 |
| Error máximo | 0.1470 |
| Correlación de Pearson | **0.8894** (p < 1e-300) |

### Curva precisión-recall LSH (Fase 3)

Con 100 firmas y 404 pares verdaderamente similares (Jaccard ≥ 0.20):

| b | r | Candidatos | TP | FP | FN | Precisión | Recall |
|---|---|---|---|---|---|---|---|
| 100 | 1  | 13,090 | 404 | 12,686 | 0   | 0.031 | 1.000 |
| 50  | 2  | 1,101  | 268 | 833    | 136 | 0.243 | 0.663 |
| **25** | **4** | **20** | **17** | **3** | **387** | **0.850** | 0.042 |
| 20  | 5  | 1      | 0   | 1      | 404 | 0.000 | 0.000 |

**Configuración óptima: b=25, r=4** — máxima precisión con recall razonable; punto más alejado del origen en la curva PR.

---

## Progreso

| Fase | Estado | Notebook |
|---|---|---|
| 0 — Entorno | ✅ Completada | `fase0_verificacion.ipynb` |
| 1 — Preprocesamiento + EDA | ✅ Completada | `fase1_preprocesamiento_eda.ipynb` |
| 2 — Procesamiento distribuido | ✅ Completada | `fase2_procesamiento_distribuido.ipynb` |
| 3 — MinHash + LSH | ✅ Completada | `fase3_minhash_lsh.ipynb` |
| 4 — Reglas de asociación | 🟡 Pendiente | `fase4_reglas_asociacion.ipynb` |
| 5 — Reporte técnico | ✅ Completada | `outputs/Reporte_Proyecto1.pdf` |

### Detalle del entorno (Fase 0)
- PySpark 4.1.1 en venv `proyecto_dm`
- SparkSession local: lee dataset, UI en `localhost:4040`
- Cluster Docker: 1 master + 2 workers · 20 cores · 13.3 GiB
- `docker-compose.yml` ajustado: solo puertos 8080 y 7077

---

## Stack técnico

- **Lenguaje:** Python 3.11
- **Procesamiento distribuido:** Apache Spark 4.1.1 (PySpark)
- **Cluster:** Docker Compose · 1 master + 2 workers
- **ML:** Spark MLlib (`MinHashLSH`, `FP-Growth`)
- **Análisis y visualización:** pandas, NumPy, matplotlib
- **Notebooks:** Jupyter
- **Hardware referencia:** Mac M1 Pro (ARM)

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

## Referencias

- Harper, F. M., & Konstan, J. A. (2015). *The MovieLens Datasets: History and Context*. ACM TiiS.
- Leskovec, J., Rajaraman, A., & Ullman, J. D. (2014). *Mining of Massive Datasets*, Cap. 3 (Finding Similar Items) y Cap. 6 (Frequent Itemsets). Cambridge University Press.
- Zaharia, M. *et al.* (2016). *Apache Spark: A Unified Engine for Big Data Processing*. Communications of the ACM 59(11).
- Han, J., Pei, J., & Yin, Y. (2000). *Mining Frequent Patterns without Candidate Generation* (FP-Growth). SIGMOD.
- Documentación oficial de [Spark MLlib](https://spark.apache.org/docs/latest/ml-features.html#minhash-for-jaccard-distance) y [FP-Growth](https://spark.apache.org/docs/latest/ml-frequent-pattern-mining.html).
