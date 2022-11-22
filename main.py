
import os
import uuid
from typing import List

from dotenv import load_dotenv
from fastapi import FastAPI
from faker import Faker
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.producer import KafkaProducer


from commands import CreatePeopleCommand
from entities import Person


load_dotenv(verbose=True)

app = FastAPI()

@app.on_event('startup')
async def startup_event():
  """_summary_: Create the topic if it doesn't exist
  """
  # Getting the boot strap servers from the environment
  client = KafkaAdminClient(bootstrap_servers=os.environ['BOOTSTRAP_SERVER'])
  try:
    # Creating the topic
    topic = NewTopic(name=os.environ['TOPIC_NAME'],
                    num_partitions=int(os.environ['TOPIC_PARTITION']),
                    replication_factor=int(os.environ['TOPIC_REPLICA']))
    client.create_topics([topic])
  except TopicAlreadyExistsError as e:
    print(e)
  finally:
    client.close()


def make_producer():
  """_summary_: Create a Kafka producer
  """
  producer = KafkaProducer(bootstrap_servers=os.environ['BOOTSTRAP_SERVER'])
  return producer


@app.post('/api/people', status_code=201, response_model=List[Person])
async def create_people(cmd: CreatePeopleCommand):
  """_summary_: Create a list of people
  """
  people: List[Person] = []
  # Creating the Fake people
  faker = Faker()
  # Creating the producer
  producer = make_producer()
  # Creating the people and sending them to the topic
  for _ in range(cmd.count):
    person = Person(id=str(uuid.uuid4()), name=faker.name(), title=faker.job().title())
    people.append(person)
    producer.send(topic=os.environ['TOPIC_NAME'],
                key=person.title.lower().replace(r's+', '-').encode('utf-8'),
                value=person.json().encode('utf-8'))
  # Closing the producer
  producer.flush()

  return people

