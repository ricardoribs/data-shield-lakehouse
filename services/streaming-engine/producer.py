import time
import json
import random
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

# --- CONFIGURAÇÕES ---
# Dentro da rede do Docker, usamos o nome do serviço "kafka"
# A porta 29092 é geralmente usada para comunicação interna entre containers
KAFKA_TOPIC = "transactions"
KAFKA_BOOTSTRAP_SERVERS = ['kafka:29092'] 

fake = Faker('pt_BR') # Gera nomes brasileiros

def serializer(message):
    return json.dumps(message).encode('utf-8')

print(f"⏳ Tentando conectar ao Kafka em: {KAFKA_BOOTSTRAP_SERVERS}...")

# Inicializa o Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=serializer
    )
    print("✅ Conectado ao Kafka com sucesso!")
except Exception as e:
    print(f"❌ Erro crítico ao conectar no Kafka: {e}")
    print("Dica: Se a porta 29092 falhar, tente mudar o código para 'kafka:9092'")
    exit(1)

# Loop Infinito de Geração de Dados
print(f"🚀 Iniciando envio de dados simulados para o tópico '{KAFKA_TOPIC}'...")

try:
    while True:
        transaction = {
            "transaction_id": fake.uuid4(),
            "client_name": fake.name(),
            "cpf": fake.cpf(),
            "amount": round(random.uniform(5.0, 5000.0), 2),
            "store_name": fake.company(),
            "category": random.choice(['Eletrônicos', 'Moda', 'Mercado', 'Farmácia', 'Restaurante']),
            "payment_method": random.choice(['Credit Card', 'Debit Card', 'Pix', 'Cash']),
            "transaction_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "city": fake.city(),
            "state": fake.state_abbr()
        }

        # Envia para o Kafka
        producer.send(KAFKA_TOPIC, transaction)
        
        # Mostra na tela para você saber que está rodando
        print(f"📤 Enviado: {transaction['client_name']} | R$ {transaction['amount']} | {transaction['store_name']}")

        # Aguarda um pouco para não travar o log (0.5 segundos)
        time.sleep(0.5)

except KeyboardInterrupt:
    print("\n🛑 Parando o gerador de dados...")
    producer.close()