import os
import csv
import json
import logging
import configparser
import kafka.errors
from kafka import KafkaProducer
from itertools import islice

logging.basicConfig(level=logging.INFO)

config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(__file__), "config", "config.ini")
config.read(config_path)

class KafkaProducerManager:
    INDEX_FILE = os.path.join(os.path.dirname(__file__), "data", "index.txt")
    CONFIG = {
        "batch_size": 32768,
        "linger_ms": 50,
        "internal_servers": ["kafka:9092"],
        "local_servers": ["localhost:9094"]
    }

    def __init__(self, topic, filepath):
        self.topic=topic
        self.filepath=filepath
        self._index = self._read_index()

    def _read_index(self):
        try:
            with open(KafkaProducerManager.INDEX_FILE, "r") as file:
                return int(file.read().strip())
        except FileNotFoundError:
            logging.warning("Index file not found. Starting from index 0")
            return 0
        except Exception as e:
            logging.error(f"Error reading index from file: {e} -> So we will return index 0")
            return 0
        
    def _write_index(self, index):
        try:
            with open(KafkaProducerManager.INDEX_FILE, "w") as file:
                file.write(str(index))
            logging.info(f"Index {index} written to file.")

        except Exception as e:
            logging.error(f"Error writing index to file")

    def _on_send_success(self, record_metadata):
        self._index += 1
        logging.info(f"Message sent successfully -> "
                     f"Topic: {record_metadata.topic}, "
                     f"Partition: {record_metadata.partition}, "
                     f"Offset: {record_metadata.offset}")
        
    def _create_producer(self):
        try:
            return KafkaProducer(
                bootstrap_servers=KafkaProducerManager.CONFIG["internal_servers"],
                value_serializer=lambda x: json.dumps(x).encode("utf-8"),
                batch_size=KafkaProducerManager.CONFIG["batch_size"],
                linger_ms=KafkaProducerManager.CONFIG["linger_ms"]
            )
        
        except kafka.errors.NoBrokersAvailable:
            logging.info(
                "We assume that we are running locally, so we will localhost:9094"
            )

            return KafkaProducer(
                bootstrap_servers=KafkaProducerManager.CONFIG["local_servers"],
                value_serializer=lambda x: json.dumps(x).encode("utf-8"),
                batch_size=KafkaProducerManager.CONFIG["batch_size"],
                linger_ms=KafkaProducerManager.CONFIG["linger_ms"]
            )
        
    def _process_message(self, producer, topic, filepath):
        try:
            with open(filepath, "r") as file:
                reader = csv.DictReader(file)

                message_count = 0
                for row in islice(reader, self._index, None):
                    try:
                        producer.send(topic, value=row).add_callback(self._on_send_success)
                        message_count += 1
                        if message_count % 2500 == 0:
                            producer.flush()

                    except Exception as e:
                        logging.error(f"Error sending message to Kafka topic: {e}")
                        continue
                        
                producer.flush()
        except FileNotFoundError:
            logging.error(f"The {filepath} did not exist")
        except Exception as e:
            logging.error(f"An unexpected error occured: {e}")
        finally:
            producer.close(timeout=10)

    def run(self):
        producer = self._create_producer()
        try:
            self._process_message(producer, self.topic, self.filepath)

        except KeyboardInterrupt:
            logging.warning("KeyboardInterrupt detected. Exiting...")
          
        finally:
            try:
                producer.flush()
            except Exception as e:
                logging.error(f"Error during producer flush: {e}")

            self._write_index(self._index)
            producer.close(timeout=5)
            logging.info(f"Producer closed. Total {self._index} messages were sent")


if __name__ == "__main__":
    filepath = os.path.join(os.path.dirname(__file__), "data", "yellow_tripdata_2024.csv")
    topic = config['kafka']['topic']
    producer = KafkaProducerManager(topic, filepath)
    producer.run()