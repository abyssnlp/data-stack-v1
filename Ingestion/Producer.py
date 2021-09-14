import os
import time
import logging.config
from rich.logging import RichHandler
from config import LOGGING_CONFIG, DATA_DIR

import pandas as pd
from kafka import KafkaProducer

# Logging
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger("root")
logger.handlers[0] = RichHandler(markup=True)


class RetailProducer:
    def __init__(self, bootstrap_servers, topic):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    @staticmethod
    def get_data():
        data: pd.DataFrame = pd.read_csv(os.path.join(DATA_DIR, "online_retail.csv"))
        for idx, row in data.iterrows():
            row_string = ','.join(map(str, row.to_list()))
            yield row_string

    def success(self, metadata):
        logger.info(f"Successfully sent message to {self.topic}")
        logger.info(metadata.topic)

    def error(self, exception):
        logger.error(f"Error sending message to {self.topic}")
        logger.error(exception)

    def async_send(self):
        data = self.get_data()
        try:
            while True:
                self.producer.send(self.topic,
                                   next(data).encode('UTF-8')).add_callback(self.success).add_errback(self.error)
                time.sleep(1)
        except StopIteration as e:
            logger.info("No more messages to send. Flushing producer ...")
        finally:
            self.producer.flush()


if __name__ == '__main__':
    producer = RetailProducer(bootstrap_servers="localhost:9092",
                              topic="bankretail")
    producer.async_send()
