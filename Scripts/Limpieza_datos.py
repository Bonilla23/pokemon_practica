from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType

spark = SparkSession.builder.appName("Limpieza_Pokemon").getOrCreate()

# 1. ESQUEMA
schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StructType([
        StructField("english", StringType(), True),
        StructField("japanese", StringType(), True),
        StructField("chinese", StringType(), True),
        StructField("french", StringType(), True)
    ]), True),
    StructField("type", ArrayType(StringType()), True),
    StructField("base", StructType([
        StructField("HP", LongType(), True),
        StructField("Attack", LongType(), True),
        StructField("Defense", LongType(), True),
        StructField("Sp. Attack", LongType(), True),
        StructField("Sp. Defense", LongType(), True),
        StructField("Speed", LongType(), True)
    ]), True),
    StructField("_corrupt_record", StringType(), True) 
])

try:
    print("--- Iniciando ingesta de datos (Capa RAW) ---")
    
    # Ingesta robusta: Soporta múltiples archivos JSON si se añaden a la carpeta
    path_raw = "Datos/Raw/*.json"
    
    df = spark.read \
        .option("multiline", "true") \
        .schema(schema) \
        .json(path_raw) \
        .cache()

    if df.count() == 0:
        raise ValueError("El dataframe está vacío. Verifique la ruta Raw.")
    
    print(f"Número de registros cargados: {df.count()}")

    # 2. LIMPIEZA INICIAL
    # Quitamos corruptos si existen y filas sin ID/Nombre
    if "_corrupt_record" in df.columns:
        df = df.filter(col("_corrupt_record").isNull())

    # 3. TRANSFORMACIÓN Y CORRECCIÓN (Aplanamiento directo)
    # En lugar de withField, extraemos cada campo y aplicamos la corrección individualmente
    # Usamos backticks `` para los campos con puntos
    df_final = df.select(
        col("id"),
        col("name.english").alias("nombre"),
        col("type").alias("tipos"),
        # Aplicamos la lógica de corrección (si es nulo o 0, ponemos 1)
        when(col("base.HP").isNull() | (col("base.HP") <= 0), 1).otherwise(col("base.HP")).alias("hp"),
        when(col("base.Attack").isNull() | (col("base.Attack") <= 0), 1).otherwise(col("base.Attack")).alias("ataque"),
        when(col("base.Defense").isNull() | (col("base.Defense") <= 0), 1).otherwise(col("base.Defense")).alias("defensa"),
        when(col("base.`Sp. Attack`").isNull() | (col("base.`Sp. Attack`") <= 0), 1).otherwise(col("base.`Sp. Attack`")).alias("ataque_especial"),
        when(col("base.`Sp. Defense`").isNull() | (col("base.`Sp. Defense`") <= 0), 1).otherwise(col("base.`Sp. Defense`")).alias("defensa_especial"),
        when(col("base.Speed").isNull() | (col("base.Speed") <= 0), 1).otherwise(col("base.Speed")).alias("velocidad")
    )

    # 4. LIMPIEZA FINAL
    df_final = df_final.dropna(subset=["id", "nombre"]).dropDuplicates(["id"])

    print("--- Proceso completado exitosamente ---")
    df_final.show(10)
    
    # 5. GUARDADO
    df_final.write.mode("overwrite").parquet("Datos/Processed/pokedex_limpia")
    print(f"Total de registros guardados: {df_final.count()}")

except Exception as e:
    print(f"ERROR CRÍTICO: {e}")

finally:
    spark.stop()