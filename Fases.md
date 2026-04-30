## Plan de desarrollo por fases

### Fase 0 — Entorno y configuración
- Activar el venv `proyecto_dm`
- Verificar que PySpark importa sin errores
- Crear una SparkSession en modo local y leer un CSV de prueba
- Levantar el cluster Docker y conectarse desde un notebook
- Confirmar que la Spark UI responde en `localhost:8080`

### Fase 1 — Preprocesamiento y EDA
- Cargar `...csv` con esquema explícito
- Inspeccionar cada archivo: esquema, tipos de datos, primeras filas, conteo de registros
- Eliminar nulos, duplicados y registros fuera de rango en cada dataset
- Unificar etiquetas de géneros (`Sci-Fi → Science Fiction`, etc.)
- Decidir y justificar el umbral de binarización (like / dislike)
- Convertir ratings a binario con ese umbral
- Guardar datos limpios en Parquet para reutilizar en fases siguientes
- **EDA — ratings:** distribución, media, mediana, moda, percentiles, boxplot
- **EDA — usuarios:** distribución de ratings por usuario, usuarios más y menos activos, curva de actividad (¿power law?)
- **EDA — películas:** distribución de ratings por película, long tail, más y menos calificadas
- **EDA — géneros:** distribución en el catálogo, géneros más calificados, heatmap géneros vs rating promedio
- **EDA — temporal:** evolución de ratings por año y mes, picos de actividad, tendencias
- **EDA — sparsity:** densidad de la matriz usuario-película, heatmap de submatriz representativa
- **EDA — tags:** palabras más frecuentes, relación entre tags y géneros
- Escribir una interpretación breve por cada hallazgo (va directo al reporte)

### Fase 2 — Procesamiento distribuido
- Contar ratings por película con DataFrame API
- Contar ratings por película con RDD estilo MapReduce
- Calcular el top 20 de películas más populares con título y rating promedio
- Medir y comparar tiempos RDD vs DataFrame
- Gráfica de barras del top 20 ordenada por popularidad
- Tabla comparativa de tiempos con análisis

### Fase 3 — Similitud, MinHash y LSH
- Elegir y justificar la representación de películas como conjuntos (usuarios / tags / combinada)
- Implementar Jaccard exacto sobre una muestra de películas
- Analizar distribución de similitudes exactas
- Construir firmas MinHash con Spark MLlib
- Variar `b` y `r` manteniendo `b × r` constante
- Medir falsos positivos y falsos negativos por configuración
- Generar curvas de precisión y recall vs número de bandas
- Comparar error MinHash vs Jaccard exacto (MAE, correlación de Pearson)

### Fase 4 — Reglas de asociación
- Construir transacciones: usuario → lista de películas con like
- Ejecutar FP-Growth con soporte y confianza justificados
- Revisar itemsets frecuentes más comunes
- Extraer mínimo 10 reglas con soporte, confianza y lift
- Scatter plot soporte vs confianza coloreado por lift

### Fase 5 — Reporte técnico (máx. 8 páginas)
- **Metodología:** describir cada fase aplicada
- **Hallazgos EDA:** gráficos con interpretación escrita
- **Resultados:** top 20, curvas LSH, tabla de reglas de asociación
- **Discusión crítica:** exactitud vs eficiencia, impacto de FP/FN en un recomendador real, cuándo vale la pena Spark vs local, cuellos de botella, aplicabilidad en Netflix / Spotify / Amazon

---