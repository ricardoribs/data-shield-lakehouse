from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

# --- CONFIGURAÇÃO ---
spark = SparkSession.builder \
    .appName("DataShield-Silver-Processing") \
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

print("🚀 Lendo dados da camada BRONZE...")

try:
    df_bronze = spark.read.format("delta").load("s3a://bronze/transactions")
    
    print("🧹 Limpando e Transformando dados...")
    
    # SELEÇÃO CORRIGIDA (SEM RENOMEAR, POIS JÁ VEM CERTO)
    df_silver = df_bronze.select(
        col("transaction_id"),
        col("client_name"),
        col("amount").cast("double"),
        col("store_name"),  # <--- LENDO DIRETO AGORA
        col("category"),
        col("payment_method"),
        col("transaction_date").cast("timestamp"),
        col("city"),
        col("state")
    ).dropDuplicates(["transaction_id"])

    qtd = df_silver.count()
    print(f"📊 Total de registros processados: {qtd}")

    if qtd > 0:
        print("💾 Salvando na camada SILVER...")
        df_silver.write.format("delta").mode("overwrite").save("s3a://silver/transactions")
        print("✅ Processamento Silver concluído com sucesso!")
    else:
        print("⚠️ Nenhum dado encontrado no Bronze.")

except Exception as e:
    print("\n❌ ERRO NO PROCESSAMENTO:", str(e))

spark.stop()