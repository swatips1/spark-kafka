import producer_server


def run_kafka_server():
    """We will get the data from local file"""
    input_file = "police-department-calls-for-service.json"

    """Setup producer"""
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="service.calls",
        bootstrap_servers="localhost:9093",
        client_id="sf.crime.data.analyzer"
    )

    return producer


def feed():
    producer = run_kafka_server()
    try:
        producer.generate_data()
    except:
        producer.counter = 0
        producer.flush()
        producer.close()

if __name__ == "__main__":
    feed()
