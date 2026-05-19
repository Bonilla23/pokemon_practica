# PROYECTO POKÉSPARK: Análisis y Clasificación de Datos con PySpark
## 1. Metodología: CRISP-DM (Adaptada a Big Data)
Se ha seleccionado la metodología **CRISP-DM** debido a su naturaleza iterativa, la cual es fundamental en proyectos de Big Data donde se suele requerir ajustes constantes en las etapas de limpieza y modelado.

*   **Comprensión del Negocio:** El objetivo es transformar datos técnicos y dispersos de la Pokédex en conocimiento táctico para jugadores competitivos y analistas de datos, permitiendo identificar arquetipos de combate de forma automatizada.
*   **Comprensión de los Datos:** Análisis de la fuente original (JSON), identificando estructuras anidadas complejas (`base` stats y nombres multilingües). Se ha priorizado el nombre en inglés para estandarización internacional.
*   **Preparación de los Datos:** Implementación de un pipeline de limpieza que incluye:
    *   Aplanado del esquema anidado.
    *   Tratamiento de registros corruptos y valores nulos/anómalos (sustitución por valores mínimos para evitar sesgos en el cálculo de stats).
    *   Estandarización de nombres de columnas al español para mejorar la legibilidad en capas superiores.
*   **Modelado:** Uso de **K-Means Clustering** para la segmentación de Pokémon. A diferencia de una clasificación manual, el modelo agrupa según 6 dimensiones estadísticas (HP, Atk, Def, etc.), revelando roles naturales no siempre evidentes.
*   **Evaluación:** Uso del **Coeficiente de Silueta** para validar la cohesión de los clusters y ajuste de los parámetros del modelo para maximizar la interpretabilidad en el negocio del juego.
*   **Despliegue:** Entrega de datasets optimizados en la capa Curated, listos para su integración directa en un ecosistema de Business Intelligence (Power BI).

## 2. Arquitectura de Datos y Almacenamiento
Se implementa una **Arquitectura por Capas** para asegurar trazabilidad y escalabilidad:

*   **Capa Raw (`Datos/Raw`):** Ingesta del JSON original. Se mantiene inmutable para permitir el reprocesamiento en caso de cambios en la lógica de negocio.
*   **Capa Processed (`Datos/Processed`):** Datos limpios y normalizados. El almacenamiento se realiza en formato **Parquet** tambien optimizamos las consultas al filtrar solo las columnas necesarias, limpiamos los datos nulos y anomalias.
*   **Capa Curated (`Datos/Curated`):** Vistas especializadas (Top Stats, Tanques, Atacantes) que eliminan la lógica de cálculo del dashboard para mejorar el rendimiento de la visualización.
*   **Capa Modelado (`Datos/Modelado`):** Guardamos los resultados del clustering en formato parquet, para su posterior análisis y visualización.

## 3 y 4. Interpretación de Clusters (K-Means) y Evaluación
Se han definido 5 clusters basados en 6 dimensiones estadísticas. La calidad del modelo se ha validado mediante el **Coeficiente de Silueta**, asegurando que los grupos sean cohesivos y bien diferenciados.

*   **Cluster 0 - Básicos o Early-game:** Pokémon con estadísticas base bajas.
*   **Cluster 1 - Atacantes Físicos:** Alta inversión en Ataque y Velocidad.
*   **Cluster 2 - Defensivos de Apoyo:** Alta Defensa y HP, orientados a resistir.
*   **Cluster 3 - Atacantes Especiales:** Daño masivo a través de Sp. Attack.
*   **Cluster 4 - Elite/Legendarios:** Estadísticas excepcionales en todas las áreas.

## 5. Visualización
En la carpeta `PowerBI/` se incluye `Pokemon.pbix`. El dashboard consume directamente los archivos Parquet, permitiendo filtros dinámicos por Cluster y Stats, facilitando la toma de decisiones para la formación de equipos competitivos.