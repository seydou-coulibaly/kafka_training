import threading
import asyncio
from random import sample, shuffle
import chardet
from concurrent.futures import ThreadPoolExecutor

import python_weather
from kafka import KafkaProducer
from datapackage import Package



def get_cities(data_size):
    """Get a list of cities name"""
    package = Package('https://datahub.io/core/world-cities/datapackage.json')

    for resource in package.resources:
        if resource.descriptor['datahub']['type'] == 'derived/csv':
            cities_data = resource.read()
            # shuffle(cities_data)
            return sample(cities_data, data_size)



async def get_weather(city):
    """Get the weather related to a given city."""
    client = python_weather.Client(unit=python_weather.IMPERIAL)
    # fetch a weather forecast from a city
    weather = await client.get(city)
    await client.close()
    return f"{city}'s located at {weather.location} and has currently temperature forcast of {weather.current.temperature} {weather.current.unit.temperature} with the description '{weather.current.description}'"


def produce_city(iterable):
    """Produce some message in the given topic"""
    topic, city = iterable
    producer_name = threading.current_thread().name
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    message = asyncio.run(get_weather(city))
    producer.send(topic=f"{topic}", value=f"{producer_name} :{message}".encode("utf-8"))
    producer.flush()


def produce_id_message(iterable):
    """Produce some message in the given topic"""
    topic, message_id = iterable
    producer_name = threading.current_thread().name
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    message_to_send = f"{producer_name} : message to {topic}'s topic where id : {message_id}"
    producer.send(
        topic=f"{topic}",
        value=message_to_send.encode("utf-8")
    )
    producer.flush()


if __name__ == "__main__":
    topic = "quickstart-events"
    
    # city data
    cities_data = get_cities(60)
    # get only the city name
    cities_data = [city_data[0] for city_data in cities_data]


    # threading
    iterators_formated = list(zip([topic]* len(cities_data), cities_data))
    # with ThreadPoolExecutor() as executor:
    with ThreadPoolExecutor(max_workers=100) as executor:
        print("number of thread = ", executor._max_workers)
        for result in executor.map(produce_city, iterators_formated):
            pass


    topic = "training"
    data = range(100)
    iterators_formated = list(zip([topic]* len(data), data))
    with ThreadPoolExecutor() as executor:
        for result in executor.map(produce_id_message, iterators_formated):
          pass