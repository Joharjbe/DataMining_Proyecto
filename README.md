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

El proyecto aplica técnicas de Data Mining sobre **MovieLens 20M** para resolver cuatro problemas (las Fases 1–3 a escala distribuida con PySpark; la Fase 4 en local sobre muestra justificada):

1. **Caracterizar el dataset** mediante EDA (distribuciones, sparsity, evolución temporal, géneros, tags).
2. **Procesamiento distribuido**: comparar RDDs vs DataFrames y obtener el top-20 de películas más calificadas.
3. **Búsqueda de pares similares** con Jaccard exacto, MinHashing y Locality-Sensitive Hashing (LSH), evaluando precisión y recall del modelo aproximado.
4. **Reglas de asociación** combinando dos enfoques: **Apriori en local** sobre muestra del 50% (Python puro, validado empíricamente contra el dataset completo) y **FP-Growth distribuido con Spark MLlib** sobre muestra del 10% como complemento — cubre los dos caminos del PDF y descubre itemsets de tamaño hasta 5 que Apriori puro no puede explorar.

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
│   ├── figures/                      ← 14 PNG generados por los notebooks
│   ├── results/
│   │   ├── ratings_clean.parquet     20M filas + columna like
│   │   ├── movies_clean.parquet      27K películas con géneros unificados
│   │   └── tags_clean.parquet        463K tags filtrados
│   ├── Reporte_Proyecto1.pdf         ← reporte técnico final
│   └── Diapositivas_Proyecto1.pptx   ← slides para sustentación
│
├── entorno.yml                       ← especificación conda (env: pySpark)
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

### 1. Activar el entorno conda
```bash
cd /Users/nahiaescalante/Documents/UTEC/2026-1/Data_Mining/Proyecto/DataMining_Proyecto
conda activate pySpark
```

> El archivo `entorno.yml` reproduce el entorno: `conda env create -f entorno.yml`

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
| 4 | `fase4_reglas_asociacion.ipynb` | Reglas Apriori (en local) + visualización sup × conf × lift |

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

### Fase 4 · Reglas de asociación

**Decisión: Apriori manual en Python puro sobre muestra del 50%.**

La rúbrica acepta explícitamente *"Apriori en local sobre muestra justificada"*. Se eligió esta vía sobre FP-Growth distribuido por restricciones de hardware (16 GB RAM, disco compartido) — Spark FP-Growth desbordaba el FP-Tree en local. El módulo se ejecuta sin Spark: `pandas + pyarrow` para carga, Python puro para Apriori.

**Pipeline:**
1. Carga directa de `ratings_clean.parquet` y `movies_clean.parquet` con `pyarrow`.
2. Universo: películas con ≥ 5,000 likes (587 películas mainstream) — filtro justificado por error estándar de la confianza.
3. Muestra del 50 % de usuarios (≈68.5K transacciones) con `seed = 42`. Justificación cuantitativa por **desigualdad de Hoeffding**: para reglas con soporte global ≥ 0.06, la probabilidad de no detectarlas en la muestra es `~10⁻⁶`; para ≥ 0.07, despreciable. **Validación empírica**: comparado con Apriori sobre 100% (137K usuarios), el 50% obtuvo 3,418 vs 3,415 reglas — diferencia despreciable que confirma Hoeffding.
4. **Apriori** en dos pasadas (1-itemsets y 2-itemsets) con poda anti-monotónica.
5. Reglas `A → B` con métricas soporte / confianza / lift; filtros `MIN_SUPPORT = 0.05`, `MIN_CONFIDENCE = 0.5`, `lift > 1`.
6. Visualización: top 15 reglas por lift + scatter `support × confidence × lift`.

**Resultado Apriori:** 3,418 reglas con `lift > 1` (la rúbrica exige ≥ 10) en 22.9 s.

**Complemento — FP-Growth distribuido con Spark MLlib:**

Para cubrir el camino A de la rúbrica y descubrir itemsets de tamaño ≥ 3 inalcanzables para Apriori puro, se ejecutó también `pyspark.ml.fpm.FPGrowth` sobre configuración más restrictiva por límites de RAM en local (probamos primero los mismos parámetros de Apriori → `OutOfMemoryError`):

- Universo: ≥ 10,000 likes (247 películas)
- Muestra: 10 % de usuarios (~13K transacciones)
- `MIN_SUPPORT = 0.10`, driver Spark = 5g, stack JVM = 100m

**Resultado FP-Growth:** 1,550 itemsets frecuentes (incluyendo **690 de tamaño ≥ 3, hasta tamaño 5**), 2,674 reglas — descubre patrones tipo `{LOTR1, LOTR2} → LOTR3` con confianza ≈ 0.90 que Apriori puro no encuentra.

**Los dos algoritmos son complementarios**, no competitivos: Apriori puro examina el rango binario sobre universo amplio; FP-Growth descubre patrones saga-completa sobre universo restringido. La comparación detallada está en secciones 3.6 (config máxima viable de cada uno) y 3.7 (apples-to-apples con parámetros idénticos) del reporte.

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

DataFrame es **consistentemente más rápido** que RDD gracias al optimizador **Catalyst** y a Tungsten. Cada operación se ejecutó 5 veces (1 cold + 4 warm con caché) para reportar speedup con desviación estándar:

| Operación | Speedup cold | Speedup warm |
|---|---|---|
| Conteo por película | 20.4× | 38.5× ± 15.9× |
| Top-20 con join | 26.5× | 62.7× ± 7.7× |
| Conteo por usuario | 44.5× | 101.9× ± 17.4× |

> Incluso en frío DataFrame es 20–45× más rápido (la ventaja no depende del caché). En caliente el caché amplifica la diferencia (38–102×): plan Catalyst reusado, datos en formato columnar Tungsten en memoria, sin re-deserializar.

### MinHash vs Jaccard exacto (Fase 3)

Sobre 19,900 pares evaluados con `t = 100` funciones hash:

| Métrica | Valor |
|---|---|
| Error promedio (MAE) | 0.0072 |
| Correlación de Pearson | **0.8223** (p < 1e-300) |

**Sensibilidad al número de hashes (`t`):** el MAE cae monotónicamente de 0.0187 (`t=10`) a 0.0037 (`t=400`), siguiendo la cota teórica `O(1/√t)`. La correlación de Pearson sube de 0.36 (`t=10`) a 0.94 (`t=400`), pasando por 0.82 (`t=100`) y 0.89 (`t=200`). `t=100` es el punto razonable de equilibrio entre precisión y costo.

### Curva precisión-recall LSH (Fase 3)

Con 100 firmas y 75 pares verdaderamente similares (Jaccard ≥ 0.5):

| b | r | Candidatos | TP | FP | FN | Precisión | Recall |
|---|---|---|---|---|---|---|---|
| 100 | 1  | 10,267 | 75 | 10,192 | 0  | 0.007 | 1.000 |
| 50  | 2  | 302    | 45 | 257    | 30 | 0.149 | 0.600 |
| **25**  | **4**  | **1** | **1** | **0** | **74** | **1.000** | 0.013 |
| 20  | 5  | 1      | 1  | 0      | 74 | 1.000 | 0.013 |
| 10  | 10 | 0      | 0  | 0      | 75 | 0.000 | 0.000 |

**Trade-off observado:** `b=25` y `b=20` entregan precisión perfecta a cambio de recall mínimo; `b=50, r=2` es la mejor opción de balance neto en este experimento (60% recall con 15% precisión).

**Spark MLlib MinHashLSH** (config: `numHashTables=100, threshold=0.80` ≡ Jaccard ≥ 0.20): retorna **5 candidatos con precisión 1.000 y recall 1.000** en 67 ms — estrictamente superior al banding manual gracias a su esquema OR de bandas optimizado.

**LSH sobre catálogo completo** (`b=25, r=4` aplicado a las 8,362 películas): 2,826 pares candidatos en 0.0 s + validación Jaccard exacta en 2.0 s; 927 pares finales con `J ≥ 0.20`. Tiempo total **2.9 s** vs ~35 millones de comparaciones del all-pairs exacto.

### Reglas de asociación (Fase 4)

#### Apriori puro 

Sobre la muestra del 50 % (68,464 transacciones, 587 películas con ≥ 5,000 likes):

| Métrica | Valor |
|---|---|
| 1-itemsets frecuentes | 399 |
| 2-itemsets frecuentes | 5,931 |
| Reglas (`conf ≥ 0.5`, `lift > 1`) | **3,418** |
| Tiempo total Apriori | 22.9 s |
| RAM peak | ~2.5 GB |

**Validación empírica del muestreo**: corrimos Apriori también con 100 % de usuarios (137K) y obtuvimos 3,415 reglas — el 50% captura el 99.9 % de las reglas del dataset completo, confirmando empíricamente la garantía Hoeffding.

**Top reglas por lift** (asociaciones más sorprendentes — el lift detecta franquicias automáticamente):

| Antecedente → Consecuente | Soporte | Confianza | Lift |
|---|---|---|---|
| Harry Potter 2 → Harry Potter 1 | 0.055 | 0.78 | **9.15** |
| Wallace & Gromit (Close Shave) → W&G (Wrong Trousers) | 0.060 | 0.78 | 8.19 |
| Bourne Supremacy → Bourne Identity | 0.064 | 0.84 | 7.02 |
| Kill Bill 2 → Kill Bill 1 | 0.097 | 0.87 | 6.73 |
| Iron Man → The Dark Knight | 0.055 | 0.78 | **5.95** |

**Hallazgo clave:** las 20 reglas con `lift > 3` son sagas o películas que comparten director / año / audiencia objetivo. *Iron Man → Dark Knight* es la única regla "no-saga" con lift extremo: ambas son películas de superhéroes adultos de 2008.

#### Spark FP-Growth distribuido

Como complemento, ejecutamos `pyspark.ml.fpm.FPGrowth` sobre configuración más restrictiva (universo ≥ 10,000 likes / 247 películas, muestra 10 % / 13K transacciones, `MIN_SUPPORT = 0.10`) — la única que evita `OutOfMemoryError` en hardware local de 16 GB:

| Métrica | Valor |
|---|---|
| Itemsets frecuentes (todos los `k`) | 1,550 |
| **Itemsets de tamaño ≥ 3** | **690** (hasta `k = 5`) |
| Reglas (`conf ≥ 0.5`, `lift > 1`) | 2,674 |
| Tiempo total | 0.3 s |

FP-Growth descubre patrones tipo `{LOTR1, LOTR2} → LOTR3` con confianza ≈ 0.90 que Apriori puro (limitado a `k = 2` por costo `O(|basket|^k)`) no encuentra.

#### Comparación apples-to-apples (mismos parámetros)

Para aislar las diferencias algorítmicas de las diferencias de configuración, corrimos también Apriori con los parámetros restrictivos de FP-Growth (universo 247 pelis, muestra 10 %, `MIN_SUPPORT = 0.10`):

| Métrica | Apriori puro | Spark FP-Growth |
|---|---|---|
| Tiempo | 1.43 s | **0.26 s** ⚡ |
| Itemsets totales | 838 (`k = 1, 2`) | 1,550 (`k = 1..5`) |
| Itemsets de `k ≥ 3` | 0 (limitado) | **690** |
| Reglas | 611 | 2,645 |
| Tamaño máx itemset | 2 | **5** |

**Conclusiones rigurosas:**
1. **FP-Growth es ~5.5× más rápido** que Apriori puro sobre datos pre-filtrados de este tamaño — su FP-Tree evita las dos pasadas explícitas de Apriori; con datos chicos el overhead de Spark se amortiza con el algoritmo más inteligente.
2. FP-Growth descubre **690 itemsets de tamaño ≥ 3** que Apriori (por nuestra decisión de limitarlo a `k = 2`) no encuentra; estos generan ~2,000 reglas adicionales con antecedente compuesto del tipo `{LOTR1, LOTR2} → LOTR3`.
3. La diferencia es **100 % atribuible al algoritmo**, no a los parámetros — los dos enfoques son complementarios.

#### Análisis cross-genre vs intra-genre 

Adicionalmente clasificamos las 3,418 reglas según los géneros de antecedente y consecuente:

| Tipo | Cantidad | % |
|---|---|---|
| Intra-género (comparten ≥1 género) | 2,292 | 67 % |
| **Cross-género (sin género común)** | **1,126** | **33 %** |

**Las 72 reglas cross-género con lift > 3** son las más valiosas para un recomendador moderno — revelan patrones que el catálogo de metadata NO predice:
- WALL·E (animación familiar) → The Dark Knight (acción adulta) — `lift = 5.28` · ambas 2008
- Vertigo → Psycho — `lift = 4.94` · Hitchcock (director, no género)
- Maltese Falcon → Casablanca — `lift = 4.42` · cine clásico años 40

**Insight:** sin metadata, las reglas detectan patrones de comportamiento (curaduría humana invisible) que el sistema de géneros no captura.

---

## Progreso

| Fase | Estado | Notebook |
|---|---|---|
| 0 — Entorno | ✅ Completada | `fase0_verificacion.ipynb` |
| 1 — Preprocesamiento + EDA | ✅ Completada | `fase1_preprocesamiento_eda.ipynb` |
| 2 — Procesamiento distribuido | ✅ Completada | `fase2_procesamiento_distribuido.ipynb` |
| 3 — MinHash + LSH | ✅ Completada | `fase3_minhash_lsh.ipynb` |
| 4 — Reglas de asociación | ✅ Completada | `fase4_reglas_asociacion.ipynb` |
| 5 — Reporte técnico | ✅ Completada | `outputs/Reporte_Proyecto1.pdf` |

### Detalle del entorno (Fase 0)
- PySpark 4.1.1 en conda env `pySpark`
- SparkSession local: lee dataset, UI en `localhost:4040`
- Cluster Docker: 1 master + 2 workers · 20 cores · 13.3 GiB
- `docker-compose.yml` ajustado: solo puertos 8080 y 7077

---

## Stack técnico

- **Lenguaje:** Python 3.11
- **Procesamiento distribuido:** Apache Spark 4.1.1 (PySpark)
- **Cluster:** Docker Compose · 1 master + 2 workers
- **ML:** Spark MLlib (`MinHashLSH`, `FPGrowth`) · Apriori manual en Python puro (Fase 4)
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
- Agrawal, R., & Srikant, R. (1994). *Fast Algorithms for Mining Association Rules in Large Databases*. VLDB. — base teórica de Apriori
- Han, J., Pei, J., & Yin, Y. (2000). *Mining Frequent Patterns without Candidate Generation* (FP-Growth). SIGMOD. — base teórica de FP-Growth
- Hoeffding, W. (1963). *Probability Inequalities for Sums of Bounded Random Variables*. Journal of the American Statistical Association.
- Documentación oficial de [Spark MLlib MinHashLSH](https://spark.apache.org/docs/latest/ml-features.html#minhash-for-jaccard-distance) y [Spark MLlib FP-Growth](https://spark.apache.org/docs/latest/ml-frequent-pattern-mining.html).
