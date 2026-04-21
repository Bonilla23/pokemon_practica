from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

spark = SparkSession.builder.appName("Modelado_Clustering").getOrCreate()

# 1. Leer datos de Curated
df = spark.read.parquet("Datos/Curated")

# 2. Preparar los datos para ML
# Seleccionamos las estadísticas base para agrupar
features = ["base.HP", "base.Attack", "base.Defense", "base.`Sp. Attack`", "base.`Sp. Defense`", "base.Speed"]
assembler = VectorAssembler(inputCols=features, outputCol="features_vector")
df_vector = assembler.transform(df)

# Escalar datos (importante para que las estadísticas con números grandes no dominen a las pequeñas)
scaler = StandardScaler(inputCol="features_vector", outputCol="features", withStd=True, withMean=False)
scaler_model = scaler.fit(df_vector)
df_scaled = scaler_model.transform(df_vector)

# 3. Aplicar K-Means (agrupamos en 5 tipos de Pokémon)
kmeans = KMeans(k=5, seed=1)
model = kmeans.fit(df_scaled)
predictions = model.transform(df_scaled)

# 4. Ver resultados
print("Resultados del Clustering (Pokémon asignados a grupos):")
predictions.select("name", "prediction").show(10)

# 5. Guardar el resultado final para la visualización
predictions.write.mode("overwrite").parquet("Datos/Modelado")