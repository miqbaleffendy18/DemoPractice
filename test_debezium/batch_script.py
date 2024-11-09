from confluent_kafka import Consumer, KafkaException
import json
import pandas as pd
from datetime import datetime
import time
import os

def export_output(df, topic, csv_output_path):
    csv_name = topic.replace('.', '_')
    csv_file_path = os.path.join(csv_output_path, f'{csv_name}.csv')
    df.to_csv(csv_file_path, sep=',', header=True, index=False)
    print(f"Exported data to {csv_file_path}")

def consume_messages(broker_server, consumer_group, topic, primary_key, csv_output_path, batch_size=1000, wait_timeout=30):
    consumer_config = {
        'bootstrap.servers': broker_server,
        'group.id': consumer_group,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }

    try:
        consumer = Consumer(consumer_config)
        consumer.subscribe([topic])
    except KafkaException as e:
        print(f"Failed to create consumer: {e}")
        return

    batch_data = {}
    start_time = time.time()

    try:
        while True:
            message = consumer.poll(timeout=5.0)

            if message:
                if message.error():
                    print(f"Consumer error: {message.error()}")
                    continue

                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"{current_time} - Current Offset: {message.offset()}, Partition: {message.partition()}, Topic: {message.topic()}")

                try:
                    message_value = json.loads(message.value().decode('utf-8'))
                    payload = message_value.get('payload', {})
                    op = payload.get('op')
                    if op in ('c', 'u'):
                        after_data = payload.get('after', {})
                        primary_key_value = after_data.get(primary_key)
                        batch_data[primary_key_value] = after_data
                except (json.JSONDecodeError, AttributeError) as e:
                    print(f"Failed to decode message: {e}")

                if len(batch_data) >= batch_size:
                    break

            if time.time() - start_time >= wait_timeout:
                elapsed_time = time.time() - start_time
                print(f"No new messages received within {elapsed_time} seconds. Exiting.")
                break

        if batch_data:
            df = pd.DataFrame(batch_data.values())
            export_output(df, topic, csv_output_path)

            consumer.commit()
            print("Offset committed.")
        else:
            print("No data found for specified operations.")

    except KafkaException as e:
        print(f"Kafka error: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    # Parameters
    broker_server = 'localhost:9093'
    consumer_group = 'consumer_group'
    topic = 'mysqldb.inventory.customers'
    primary_key = 'id'
    csv_output_path = './data_output'

    consume_messages(broker_server, consumer_group, topic, primary_key, csv_output_path)