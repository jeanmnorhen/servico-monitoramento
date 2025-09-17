import os
import json
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from confluent_kafka import Consumer, KafkaException

app = Flask(__name__)
CORS(app) 


# --- InfluxDB Configuration ---
influxdb_client = None
influxdb_write_api = None
influxdb_bucket = None
try:
    influxdb_url = os.environ.get('INFLUXDB_URL')
    influxdb_token = os.environ.get('INFLUXDB_TOKEN')
    influxdb_org = os.environ.get('INFLUXDB_ORG')
    influxdb_bucket = os.environ.get('INFLUXDB_BUCKET')

    if influxdb_url and influxdb_token and influxdb_org and influxdb_bucket:
        influxdb_client = InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org)
        influxdb_write_api = influxdb_client.write_api(write_options=SYNCHRONOUS)
        print("InfluxDB inicializado com sucesso.")
    else:
        print("Variáveis de ambiente do InfluxDB não encontradas.")
except Exception as e:
    print(f"Erro ao inicializar InfluxDB: {e}")

# --- Kafka Consumer Configuration ---
def create_kafka_consumer():
    try:
        kafka_conf = {
            'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVER'),
            'group.id': 'monitoring_service_group_v2', # Use a unique group.id
            'auto.offset.reset': 'earliest',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': os.environ.get('KAFKA_API_KEY'),
            'sasl.password': os.environ.get('KAFKA_API_SECRET')
        }
        if kafka_conf['bootstrap.servers']:
            consumer = Consumer(kafka_conf)
            consumer.subscribe(['eventos_ofertas'])
            print("Consumidor Kafka inicializado com sucesso.")
            return consumer
        else:
            print("Variáveis de ambiente do Kafka não encontradas para o consumidor.")
            return None
    except Exception as e:
        print(f"Erro ao inicializar Consumidor Kafka: {e}")
        return None

# --- API Routes ---

@app.route('/api/monitoring/consume', methods=['POST', 'GET'])
def consume_and_write_prices():
    # Security check for cron job
    auth_header = request.headers.get('Authorization')
    cron_secret = os.environ.get('CRON_SECRET')
    if not cron_secret or auth_header != f'Bearer {cron_secret}':
        return jsonify({"error": "Unauthorized"}), 401

    if not influxdb_write_api:
        return jsonify({"error": "InfluxDB não está inicializado."}),
    
    consumer = create_kafka_consumer()
    if not consumer:
        return jsonify({"error": "Consumidor Kafka não pôde ser criado."}),

    messages_processed = 0
    try:
        msgs = consumer.consume(num_messages=50, timeout=10.0)
        if not msgs:
            return jsonify({"status": "No new messages to process"}), 200

        points_to_write = []
        for msg in msgs:
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue
            
            try:
                event_data = json.loads(msg.value().decode('utf-8'))
                data = event_data.get('data', {})
                
                product_id = data.get('product_id')
                offer_price = data.get('offer_price')
                timestamp = event_data.get('timestamp', datetime.utcnow().isoformat())

                if product_id and offer_price is not None:
                    point = Point("offer_price") \
                        .tag("product_id", product_id) \
                        .field("price", float(offer_price)) \
                        .time(timestamp)
                    points_to_write.append(point)
                    messages_processed += 1
            except (json.JSONDecodeError, ValueError, TypeError) as e:
                print(f"Erro ao processar mensagem: {e} - Mensagem: {msg.value()}")

        if points_to_write:
            influxdb_write_api.write(bucket=influxdb_bucket, org=os.environ.get('INFLUXDB_ORG'), record=points_to_write)
            print(f"{len(points_to_write)} pontos de preço escritos no InfluxDB.")

    except Exception as e:
        return jsonify({"error": f"Erro durante o consumo de eventos: {e}"}), 500
    finally:
        consumer.close()

    return jsonify({"status": "ok", "messages_processed": messages_processed}), 200


@app.route('/api/monitoring/prices', methods=['GET'])
def get_price_history():
    if not influxdb_client:
        return jsonify({"error": "InfluxDB não está inicializado."}), 503

    product_id = request.args.get('product_id')
    if not product_id:
        return jsonify({"error": "Parâmetro 'product_id' é obrigatório."}), 400

    query_api = influxdb_client.query_api()
    query = f'''
        from(bucket: "{influxdb_bucket}")
          |> range(start: -30d)
          |> filter(fn: (r) => r._measurement == "offer_price")
          |> filter(fn: (r) => r.product_id == "{product_id}")
    '''
    
    try:
        tables = query_api.query(query, org=os.environ.get('INFLUXDB_ORG'))
        results = []
        for table in tables:
            for record in table.records:
                results.append({
                    "time": record.get_time().isoformat(),
                    "price": record.get_value(),
                    "product_id": record.values.get("product_id")
                })
        return jsonify({"data": results}), 200
    except Exception as e:
        print(f"Error querying InfluxDB: {e}")
        return jsonify({"error": f"Erro ao buscar histórico de preços: {e}"}), 500

# --- Health Check (para Vercel) ---
@app.route('/api/health', methods=['GET'])
def health_check():
    influx_status = "error"
    if influxdb_client:
        try:
            influxdb_client.ping()
            influx_status = "ok"
        except Exception:
            pass
    
    kafka_consumer = create_kafka_consumer()
    kafka_status = "error"
    if kafka_consumer:
        kafka_status = "ok"
        try:
            kafka_consumer.close()
        except Exception as e:
            print(f"Error closing Kafka consumer during health check: {e}")


    status = {
        "influxdb": influxdb_status,
        "kafka_consumer": kafka_status
    }
    http_status = 200 if all(s == "ok" for s in status.values()) else 503
    return jsonify(status), http_status

if __name__ == '__main__':
    app.run(debug=True)
