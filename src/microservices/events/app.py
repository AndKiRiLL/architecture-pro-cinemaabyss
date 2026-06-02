import json
import logging
import threading
import os
import sys
from datetime import datetime
from flask import Flask, request, jsonify
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("EventsService")

app = Flask(__name__)

# Конфигурация Kafka из переменных окружения
KAFKA_BROKERS = os.environ.get('KAFKA_BROKERS', 'kafka:9092')
TOPICS = {
    "user": "user-events",
    "payment": "payment-events",
    "movie": "movie-events"
}

# --- Инициализация Kafka ---
def create_topics(brokers, topics, num_partitions=1, replication_factor=1):
    """Создает топики, если они не существуют."""
    admin_client = None
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=brokers, client_id='events_admin')
        existing_topics = admin_client.list_topics()
        new_topics = [NewTopic(name=t, num_partitions=num_partitions, replication_factor=replication_factor) 
                      for t in topics if t not in existing_topics]
        if new_topics:
            admin_client.create_topics(new_topics=new_topics, validate_only=False)
            logger.info(f"Created topics: {[t.name for t in new_topics]}")
    except NoBrokersAvailable:
        logger.error(f"Kafka broker not available at {brokers}. Exiting.")
        sys.exit(1)
    except TopicAlreadyExistsError:
        pass
    except Exception as e:
        logger.warning(f"Could not ensure topics exist: {e}")
    finally:
        if admin_client:
            admin_client.close()

def get_kafka_producer(brokers):
    """Создает и возвращает KafkaProducer."""
    return KafkaProducer(
        bootstrap_servers=brokers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(0, 10, 1)
    )

def consume_events(brokers):
    """Функция, работающая в фоновом потоке для чтения сообщений из всех топиков."""
    consumer = None
    try:
        consumer = KafkaConsumer(
            *TOPICS.values(),
            bootstrap_servers=brokers,
            auto_offset_reset='earliest',
            group_id='events-service-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            api_version=(0, 10, 1)
        )
        logger.info("Kafka Consumer started. Listening for events...")
        for message in consumer:
            logger.info(f"Received event from topic '{message.topic}': {json.dumps(message.value, indent=2)}")
            # Здесь можно добавить любую бизнес-логику обработки события
    except NoBrokersAvailable:
        logger.error(f"Kafka consumer cannot connect to broker at {brokers}. Exiting thread.")
    except Exception as e:
        logger.error(f"Unexpected error in consumer: {e}", exc_info=True)
    finally:
        if consumer:
            consumer.close()
            logger.info("Kafka Consumer closed.")

# Инициализация Kafka-инфраструктуры при старте
create_topics(KAFKA_BROKERS, list(TOPICS.values()))
producer = get_kafka_producer(KAFKA_BROKERS)

# Запускаем консьюмера в отдельном демон-потоке
consumer_thread = threading.Thread(target=consume_events, args=(KAFKA_BROKERS,), daemon=True)
consumer_thread.start()

# --- Flask API Endpoints ---

@app.route('/api/events/health', methods=['GET'])
def health_check():
    return jsonify({"status": True, "service": "events-service"}), 200

@app.route('/api/events/user', methods=['POST'])
def create_user_event():
    data = request.get_json()
    if not data:
        return jsonify({"status": "error", "message": "Request must be JSON"}), 400

    event = {
        "type": "user",
        "event_data": data,
        "timestamp": datetime.utcnow().isoformat() + 'Z'
    }
    
    producer.send(TOPICS['user'], value=event)
    logger.info(f"Produced user event: {json.dumps(event)}")
    return jsonify({"status": "success", "message": "User event created"}), 201

@app.route('/api/events/payment', methods=['POST'])
def create_payment_event():
    data = request.get_json()
    if not data:
        return jsonify({"status": "error", "message": "Request must be JSON"}), 400

    event = {
        "type": "payment",
        "event_data": data,
        "timestamp": datetime.utcnow().isoformat() + 'Z'
    }
    
    producer.send(TOPICS['payment'], value=event)
    logger.info(f"Produced payment event: {json.dumps(event)}")
    return jsonify({"status": "success", "message": "Payment event created"}), 201

@app.route('/api/events/movie', methods=['POST'])
def create_movie_event():
    data = request.get_json()
    if not data:
        return jsonify({"status": "error", "message": "Request must be JSON"}), 400

    event = {
        "type": "movie",
        "event_data": data,
        "timestamp": datetime.utcnow().isoformat() + 'Z'
    }
    
    producer.send(TOPICS['movie'], value=event)
    logger.info(f"Produced movie event: {json.dumps(event)}")
    return jsonify({"status": "success", "message": "Movie event created"}), 201

if __name__ == '__main__':
    logger.info("Starting Events Microservice...")
    # Не используем debug=True, т.к. это перезагрузит приложение и создаст новые потоки
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8082)))