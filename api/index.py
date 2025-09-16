import os
import json
from flask import Flask, request, jsonify
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
from confluent_kafka import Consumer, KafkaException

app = Flask(__name__)
CORS(app)

# --- InfluxDB Configuration ---
influxdb_client = None
try:
    influxdb_url = os.environ.get('INFLUXDB_URL')
    influxdb_token = os.environ.get('INFLUXDB_TOKEN')
    influxdb_org = os.environ.get('INFLUXDB_ORG')
    influxdb_bucket = os.environ.get('INFLUXDB_BUCKET')

    if influxdb_url and influxdb_token and influxdb_org and influxdb_bucket:
        influxdb_client = InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org)
        print("InfluxDB inicializado com sucesso.")
    else:
        print("Variáveis de ambiente do InfluxDB não encontradas.")
except Exception as e:
    print(f"Erro ao inicializar InfluxDB: {e}")

# --- Kafka Consumer Configuration (for indexing events) ---
consumer = None
try:
    kafka_conf = {
        'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVER'),
        'group.id': 'monitoring_service_group',
        'auto.offset.reset': 'earliest',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.environ.get('KAFKA_API_KEY'),
        'sasl.password': os.environ.get('KAFKA_API_SECRET')
    }
    if kafka_conf['bootstrap.servers']:
        consumer = Consumer(kafka_conf)
        # Subscribe to relevant topics
        consumer.subscribe(['eventos_ofertas'])
        print("Consumidor Kafka inicializado com sucesso.")
    else:
        print("Variáveis de ambiente do Kafka não encontradas para o consumidor.")
except Exception as e:
    print(f"Erro ao inicializar Consumidor Kafka: {e}")

# --- API Routes ---

@app.route('/api/monitoring/prices', methods=['GET'])
def get_price_history():
    if not influxdb_client:
        return jsonify({"error": "InfluxDB não está inicializado."}), 503

    product_id = request.args.get('product_id')
    if not product_id:
        return jsonify({"error": "Parâmetro 'product_id' é obrigatório."}), 400

    query_api = influxdb_client.query_api()
    query = f'from(bucket: "{influxdb_bucket}") \
              |> range(start: -30d) \
              |> filter(fn: (r) => r._measurement == "offer_price" and r.product_id == "{product_id}") \
              |> yield(name: "mean")'
    
    try:
        tables = query_api.query(query, org=influxdb_org)
        results = []
        for table in tables:
            for record in table.records:
                results.append({
                    "time": record.get_time().isoformat(),
                    "price": record.get_value(),
                    "product_id": record["product_id"]
                })
        return jsonify({"data": results}), 200
    except Exception as e:
        return jsonify({"error": f"Erro ao buscar histórico de preços: {e}"}), 500

# --- Health Check (para Vercel) ---
@app.route('/api/health', methods=['GET'])
def health_check():
    influx_status = "error"
    if influxdb_client:
        try:
            # Ping InfluxDB to check connection
            influxdb_client.ping()
            influx_status = "ok"
        except Exception:
            pass

    status = {
        "influxdb": influx_status,
        "kafka_consumer": "ok" if consumer else "error" # More robust check needed for Kafka
    }
    http_status = 200 if all(s == "ok" for s in status.values()) else 503
    return jsonify(status), http_status

if __name__ == '__main__':
    app.run(debug=True)
p.run(debug=True)
