from time import sleep
import json

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import psycopg2 as pg


def success_error(err, msg):
    if err is None:
        print('update {} successfully sent to {} and partition [{}] at offset {}]'
              .format(msg.key(), msg.topic(), msg.partition(), msg.offset()))
    else:
        print('update message {} failed to sent, error : {}'.format(msg.key(), err))


# Confluent Kafka configuration
conf_kafka_config = {
    'bootstrap.servers': 'pkc-9q8rv.ap-south-2.aws.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'Z4SWFP2AJCRCJVYS',
    'sasl.password': '8n6LGtuEHOH9Jz2e6ea3RpJt2qt5by0RS/oI1HidrBpx6PzycSwUFMtcd12LSYKD'
}

# Schema Registry client
schema_registry_client = SchemaRegistryClient({
    'url': 'https://psrc-knmwm.us-east-2.aws.confluent.cloud',
    'basic.auth.user.info': '{}:{}'.format('2KYQLVPLVROWZYOH',
                                           'lnk3VnEbyBqZxGa9d6lG4YVrvhFCFZo8B2PrzLBc/Jt359JTpolJV7fBaJTFqeSX')
})


# Database_connection

conn = pg.connect(host='localhost', port='5432', user='postgres', password='1234', database='grow_data')
cursor = conn.cursor()

# Fetch the latest Avro schema for the value
subject_name = 'kafka_topic-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Serializer for the value
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Define the SerializingProducer
producer = SerializingProducer({
    'bootstrap.servers': conf_kafka_config['bootstrap.servers'],
    'security.protocol': conf_kafka_config['security.protocol'],
    'sasl.mechanisms': conf_kafka_config['sasl.mechanisms'],
    'sasl.username': conf_kafka_config['sasl.username'],
    'sasl.password': conf_kafka_config['sasl.password'],
    'key.serializer': key_serializer,  # Key will be serialized as a string
    'value.serializer': avro_serializer  # Value will be serialized as Avro
})


last_read_timestamp_var = {}

try:
    with open('config_last_read_timestamp_var.json') as d:
        last_read_timestamp_var = json.load(d)
        last_read_timestamp = last_read_timestamp_var.get('last_read_timestamp')

        if last_read_timestamp is None:
            last_read_timestamp = '2000-01-01 00:00:00'

except FileNotFoundError:
    pass

query = "SELECT * FROM product WHERE last_updated > '{}'".format(last_read_timestamp)
cursor.execute(query)
records = cursor.fetchall()
if not records:
    print("No records to fetch.")
else:
    # Iterate over the cursor and produce to Kafka
    for each in records:
        # Get the column names from the cursor description
        columns = [column[0] for column in cursor.description]
        print(columns)
        # Create a dictionary from the record values
        value = dict(zip(columns, each))
        print(value)
        # Produce to Kafka
        producer.produce(topic='kafka_topic', key=str(value['id']), value=value, on_delivery=success_error)
        producer.flush()

# Fetch any remaining rows to consume the result
cursor.fetchall()

query = "SELECT MAX(last_updated) FROM product"
cursor.execute(query)

# Fetch the result
result = cursor.fetchone()
max_date = result[0]  # Assuming the result is a single value

# Convert datetime object to string representation
max_date_str = max_date.strftime("%Y-%m-%d %H:%M:%S")

# Update the value in the config.json file
last_read_timestamp_var['last_read_timestamp'] = max_date_str

with open('config_last_read_timestamp_var.json', 'w') as file:
    json.dump(last_read_timestamp_var, file)

cursor.close()
conn.close()

print("Data successfully published to Kafka")
sleep(2)
