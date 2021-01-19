import asyncio

from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic

BROKER_URL = 'PLAINTEXT://localhost:9092'
MSG_TO_PROCESS = 5
TIMEOUT = 1.0
GROUP_ID = 0
TOPIC_NAME = 'service.calls'

async def consume(topic_name):
    consumer = Consumer({
        "bootstrap.servers": BROKER_URL,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest"
    })
    
    consumer.subscribe([topic_name])
    
    while True:
        messages = consumer.consume(MSG_TO_PROCESS, TIMEOUT) 
        for message in messages:
            if message is None:
                print('Message not found')
            elif message.error() is not None:
                print(f'Consumer has error(s): {message.error()}')
            else:
                print(f"Successfully consumed message {message.key()}: {message.value()}")
                
        await asyncio.sleep(TIMEOUT)
                
def run_consumer():
    try:
        asyncio.run(consume(TOPIC_NAME))
        
    except KeyboardInterrupt as e:
        print("Shutting down...")
        
if __name__ == '__main__':
    run_consumer()