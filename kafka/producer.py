from confluent_kafka import Producer
import json
import ccloud_lib
import numpy as np
import time
import requests

# Initialize configurations from "python.config" file
CONF = ccloud_lib.read_ccloud_config("python.config")
TOPIC = "cluster1" 

# Create Producer instance
producer_conf = ccloud_lib.pop_schema_registry_params_from_config(CONF)
producer = Producer(producer_conf)

# Create topic if it doesn't already exist
ccloud_lib.create_topic(CONF, TOPIC)

try:
    # Starts an infinite while loop that retrieves data from the API and produces it to the Kafka topic
    while True:
        # Make a request to the API to retrieve data
        response = requests.get('https://real-time-payments-api.herokuapp.com/current-transactions/')
        if response.status_code == 200:
            # Extract the data from the response
            data = response.json()
            
            # Process the data as needed
            record_key = "fraud"
            record_value = json.dumps(data)

            print("Producing record: {}\t{}".format(record_key, record_value))

            # Send the data to the Kafka topic
            producer.produce(
                TOPIC,
                key=record_key,
                value=record_value,
            )
            time.sleep(0.5)
            
        else:
            print("Failed to retrieve data from the API")

 # Interrupt infinite loop when hitting CTRL+C
except KeyboardInterrupt:
    pass
finally:
    producer.flush() # Finish producing the latest event before stopping the whole script