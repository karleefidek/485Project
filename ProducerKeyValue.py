from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import csv

AvroProducerConf = {'bootstrap.servers': 'localhost:9092',
                    'schema.registry.url': 'http://localhost:8081',
                   }

schema_Path = 'C:/Users/kfide/OneDrive/Documents/GitHub/485Project/schema1.avsc'
file_path = 'C:/Users/kfide/OneDrive/Documents/GitHub/485Project/2019-01.csv'

value_schema_path = avro.load(schema_Path)
key_schema = avro.loads('{"type":"string"}')
avroProducer = AvroProducer(AvroProducerConf, default_key_schema=key_schema, default_value_schema=value_schema_path)

with open(file_path) as file:
    reader = csv.DictReader(file, delimiter=',')
    
    for row in reader:
        avroProducer.produce(topic = "tripsdata", value = row, key = row['vendorid'] + row['tpep_pickup_datetime'] + row['tpep_dropoff_datetime'])

        avroProducer.flush() # STREAM IN REAL TIME