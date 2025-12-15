from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, desc

# --- CONFIGURAÇÃO CORRIGIDA (ADMIN/PASSWORD) ---
spark = SparkSession.builder \
    .appName("DataShield-Gold-Processing") \
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

print("🚀 Lendo dados da camada SILVER...")
try:
    df_silver = spark.read.format("delta").load("s3a://silver/transactions")

    # --- KPI 1: Vendas por Loja (Merchant) ---
    print("📊 Calculando KPI: Vendas por Loja...")
    df_vendas_loja = df_silver.groupBy("store_name") \
        .agg(
            sum("amount").alias("total_vendas"),
            count("transaction_id").alias("qtd_transacoes")
        ) \
        .orderBy(desc("total_vendas"))

    print("💾 Salvando na camada GOLD...")
    df_vendas_loja.write.format("delta").mode("overwrite").save("s3a://gold/vendas_por_loja")
    
    print("✅ Tabela 'vendas_por_loja' salva!")
    print("\n--- RESUMO DE VENDAS POR LOJA ---")
    df_vendas_loja.show(10, truncate=False)

except Exception as e:
    print("❌ ERRO NA GOLD:", str(e))

spark.stop()