from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans # Usaremos KMeans para simplificar (agrupa comportamentos)

# --- CONFIGURAÇÃO ---
spark = SparkSession.builder \
    .appName("DataShield-ML-Training") \
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

print("🤖 Iniciando Treinamento de Modelo de Detecção de Anomalias...")

try:
    # 1. Ler dados da Silver (Transações detalhadas)
    print("📥 Carregando dados da Silver...")
    df = spark.read.format("delta").load("s3a://silver/transactions")
    
    # 2. Preparar Features (Vamos usar o Valor da Transação para detectar anomalias)
    assembler = VectorAssembler(inputCols=["amount"], outputCol="features")
    df_features = assembler.transform(df)

    # 3. Treinar Modelo (K-Means com 2 clusters: "Normal" e "Alto Valor/Suspeito")
    # Em um cenário real, usariamos IsolationForest, mas o KMeans é nativo e rápido no Spark
    print("🧠 Treinando modelo K-Means...")
    kmeans = KMeans().setK(2).setSeed(1).setFeaturesCol("features")
    model = kmeans.fit(df_features)

    # 4. Fazer Predições
    predictions = model.transform(df_features)

    # 5. Identificar qual cluster tem a média maior (o cluster de "Alto Valor")
    centers = model.clusterCenters()
    # O cluster com o centroide maior é o de transações altas
    high_value_cluster = 0 if centers[0][0] > centers[1][0] else 1
    
    print(f"🧐 Cluster de 'Alto Valor/Suspeito' identificado: {high_value_cluster}")

    # 6. Marcar no DataFrame
    df_result = predictions.withColumn("is_anomaly", 
                                       when(col("prediction") == high_value_cluster, lit("SUSPEITA"))
                                       .otherwise(lit("NORMAL"))) \
                           .select("transaction_id", "client_name", "amount", "store_name", "is_anomaly")

    # 7. Salvar tabela de Predições
    print("💾 Salvando tabela de Inteligência (Machine Learning)...")
    df_result.write.format("delta").mode("overwrite").save("s3a://gold/ml_fraud_detection")
    
    print("✅ Processo de ML concluído com sucesso!")
    print("\n--- AMOSTRA DE TRANSAÇÕES SUSPEITAS ---")
    df_result.filter(col("is_anomaly") == "SUSPEITA").show(5)

except Exception as e:
    print("❌ Erro no treinamento:", str(e))

spark.stop()