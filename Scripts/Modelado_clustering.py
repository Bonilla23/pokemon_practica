from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

spark = SparkSession.builder.appName("Modelado_Clustering").getOrCreate()

# 1. Leer datos
df = spark.read.parquet("Datos/Processed/pokedex_limpia")

# 2. Preparar los datos para ML
# He cambiado los nombres de las columnas según tu mensaje de error
features_clean = ["hp", "ataque", "defensa", "ataque_especial", "defensa_especial", "velocidad"]

assembler = VectorAssembler(inputCols=features_clean, outputCol="features_raw")
df_vector = assembler.transform(df)

# 3. Escalar datos
scaler = StandardScaler(inputCol="features_raw", outputCol="features_scaled", withStd=True, withMean=True)
scaler_model = scaler.fit(df_vector)
df_scaled = scaler_model.transform(df_vector)

# 4. Aplicar K-Means (5 clusters)
kmeans = KMeans(featuresCol="features_scaled", k=5, seed=42)
model = kmeans.fit(df_scaled)
predictions = model.transform(df_scaled)

# 5. Preparar salida para Power BI
# Calculamos el Total aquí mismo para asegurarnos de que exista para el Top 5
resultado_final = predictions.withColumn(
    "total_stats", 
    F.col("hp") + F.col("ataque") + F.col("defensa") + 
    F.col("ataque_especial") + F.col("defensa_especial") + F.col("velocidad")
).select(
    F.col("nombre"), # Usamos "nombre" según tu error
    "hp", "ataque", "defensa", "ataque_especial", "defensa_especial", "velocidad",
    "total_stats",
    F.col("prediction").alias("cluster_id")
)

# 6. Guardar el resultado final
resultado_final.coalesce(1).write.mode("overwrite").parquet("Datos/Modelado")

print("¡Listo! Clustering completado con columnas en español.")