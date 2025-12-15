from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# --- CONFIGURAÇÃO ---
spark = SparkSession.builder \
    .appName("DataShield-Bronze-Ingestion") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Schema exato do producer.py
json_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("client_name", StringType(), True),
    StructField("cpf", StringType(), True),
    StructField("amount", FloatType(), True),
    StructField("store_name", StringType(), True),  # <--- O NOME CORRETO AQUI
    StructField("category", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True)
])

# Leitura do Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse do JSON
df_parsed = df_kafka.select(from_json(col("value").cast("string"), json_schema).alias("data")).select("data.*")

# Escrita no MinIO (Bronze)
query = df_parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://bronze/checkpoints/transactions") \
    .option("path", "s3a://bronze/transactions") \
    .start()

print("💾 Ingestão iniciada... Escutando Kafka e gravando no MinIO (Bronze).")
query.awaitTermination()