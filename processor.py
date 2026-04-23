from kafka import KafkaConsumer, KafkaProducer
from const import BROKER_ADDR, BROKER_PORT
import sys

# args
if len(sys.argv) < 3:
    print('Usage: python3 processor.py <input_topic> <output_topic>')
    exit(1)

input_topic  = sys.argv[1]   # tópico de entrada  (topic1)
output_topic = sys.argv[2]   # tópico de saída    (topic2)

# Consumidor direto (producer)
consumer = KafkaConsumer(
    input_topic,
    bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT],
    auto_offset_reset='earliest',   # lê desde o início do tópico
    group_id='processor-group',     # permite retomar de onde parou
)

# Atuar como producer para o consumer
producer = KafkaProducer(
    bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT]
)

print(f'[processor] {input_topic} --> {output_topic}')
print('[processor] aguardando mensagens...\n')

# Fazer o ciclo para todas as mensagens recebidas
for msg in consumer:
    original = msg.value.decode() # Mensagem recebida

    num_msg = int(original.split('My ')[1].split("st ")[0])
    processed = f"Minha {num_msg}º transformada do topico 1"

    print(f'Recebido: {original}')
    print(f'Publicando: {processed}\n')

    producer.send(output_topic, value=processed.encode()) # Enviar nova mensage

# flush garante que todas as mensagens saíram antes de encerrar
producer.flush()
