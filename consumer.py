import chardet
from kafka import KafkaConsumer

def consume(topic=None, **kwargs):
    """Produce some message in the given topic"""
    consumer = KafkaConsumer(topic, bootstrap_servers='localhost:9092', auto_offset_reset="earliest", **kwargs)
    consumer = KafkaConsumer(topic, bootstrap_servers='localhost:9092', auto_offset_reset="latest", **kwargs)
    
    for msg in consumer:
        encoding = chardet.detect(msg.value).get("encoding", "utf-8")
        try:
            print(msg.value.decode(encoding))
        except:
            print(f"Unable to decode the text to consume ({msg.value})")



if __name__ == "__main__":
    # consume(topic="training")
    consume(topic="quickstart-events")