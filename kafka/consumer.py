from confluent_kafka import Consumer, Producer
import json
import ccloud_lib
import joblib
from model_def import OurPipeline
import pandas as pd
import time
from send_email import Email

# Read arguments and configurations and initialize
args = ccloud_lib.parse_args()
config_file = args.config_file
topic = args.topic
conf = ccloud_lib.read_ccloud_config(config_file)

# Create Consumer instance
# 'auto.offset.reset=earliest' to start reading from the beginning of the
# topic if no committed offsets exist
consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
consumer_conf['group.id'] = 'fraud_detection'
consumer_conf['auto.offset.reset'] = 'earliest'
consumer = Consumer(consumer_conf)

# Subscribe to topic
consumer.subscribe([topic])

# Process messages
total_count = 0
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            # No message available within timeout.
            # Initial message consumption may take up to
            # `session.timeout.ms` for the consumer group to
            # rebalance and start consuming
            print("Waiting for message or event/error in poll()")
            continue
        elif msg.error():
            print('error: {}'.format(msg.error()))
        else:
            # Check for Kafka message
            record_key = msg.key()
            record_value = msg.value()
            
            data = pd.read_json(json.loads(record_value), orient='split')
            print(f"Consumed record with key {record_key} and value {data}")      
                        
            # import data into a csv file
            #data.to_csv('data.csv', index=False)
            
            # data engineering
            transformed_dataset = OurPipeline.ourpipeline(data)
            
            # preprocessor & model loading
            preprocessor = joblib.load('preprocessor.pkl')
            model = joblib.load('decision_tree.pkl')

            # data preprocessing
            features_list = ['category', 'amt', 'gender', 'city_pop', 'job', 'age',
            'year', 'month', 'day', 'dayofweek', 'hour', 'distance']
            X = data.loc[:, features_list]
            X_transformed = preprocessor.transform(X)

            # fraud labels prediction using the loaded model
            Y_pred = model.predict(X_transformed)
            
            # 'predcition' column creation
            prediction = []

            for i in data:
                prediction.append(Y_pred)

            data["prediction"] = Y_pred

            # final data
            data_final = data[["current_time", "first", "last", "city", "trans_num", "amt", "prediction"]]
            print(data_final)

            # convert to json
            record_value2 = data_final.to_json(orient='split')
            print(record_value2)
          
            time.sleep(0.5)  # Wait for 5 seconds before polling for the next message
             
            # Initialize configurations from "python.config" file
            CONF = ccloud_lib.read_ccloud_config("python.config")
            TOPIC2 = "fraud_detection_predict"
            
            # Create Producer instance
            producer_conf = ccloud_lib.pop_schema_registry_params_from_config(CONF)
            producer = Producer(producer_conf)
            
            # Create topic if it doesn't already exist
            ccloud_lib.create_topic(CONF, TOPIC2)
            
        producer.produce(
            TOPIC2,
            key=record_key, 
            value=record_value2
        )
                # p.poll() serves delivery reports (on_delivery)
        # from previous produce() calls thanks to acked callback
        producer.poll(0)
        producer.flush() # Finish producing the latest event before stopping the whole script
        print("\n*** Data consumed *** \n \n {} {} \n".format(record_key, record_value2) )
        time.sleep(0.5) # Wait half a second

        # Send email function
        Email.send_email_if_fraud(data_final)
            
except KeyboardInterrupt:
    pass
finally:
    # Leave group and commit final offsets
    consumer.close()