import os
import logging.config
from rich.logging import RichHandler
from config import LOGGING_CONFIG

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.io.hadoopfilesystem import HadoopFileSystem
from apache_beam.options.pipeline_options import PipelineOptions

# Logging
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger("root")
logger.handlers[0] = RichHandler(markup=True)


class Kafka2HDFS:
    def __init__(self, kafka_topic, bootstrap_servers,
                 output_dir):
        self.kafka_topic = kafka_topic
        self.bootstrap_servers = bootstrap_servers
        self.output_dir = output_dir
        self.window_size = 15
        self.pipeline_options = PipelineOptions(
            topic=self.kafka_topic,
            bootstrap_servers=self.bootstrap_servers,
            output_dir=self.output_dir,
            save_main_session=True,
            streaming=True
        )
        self.pipeline = beam.Pipeline(options=self.pipeline_options)

    @staticmethod
    def kafka_record_to_dict(record):
        if hasattr(record, 'value'):
            retail_bytes = record.value
        elif isinstance(record, tuple):
            retail_bytes = record[1]
        else:
            raise RuntimeError(f"Unknown record type: {type(record)}")
        import ast
        retail_message = ast.literal_eval(retail_bytes.decode('UTF-8'))
        output = {k: v for k, v in zip(retail_message.split(","), ["invoice_number", "stock_code", "description",
                                                                   "quantity", "invoice_date", "unit_price",
                                                                   "customer_id",
                                                                   "country"])}
        if hasattr(record, "timestamp"):
            output["timestamp"] = record.timestamp
        return output

    def read_from_kafka(self):
        return (self.pipeline
                | ReadFromKafka(
                    consumer_config={"bootstrap.servers": self.bootstrap_servers},
                    topics=[self.kafka_topic],
                    with_metadata=True
                )
                | beam.Map(lambda record: self.kafka_record_to_dict(record))
                )

    def write_to_hdfs(self) -> None:
        lines = self.read_from_kafka()
        lines | beam.io.WriteToText(self.output_dir, file_name_suffix=".json")
        self.pipeline.run()


if __name__ == '__main__':
    kafka_hdfs_stream = Kafka2HDFS(
        kafka_topic="bankretail",
        bootstrap_servers="localhost:9092",
        output_dir="hdfs://beam_output_v1/bank_retail"
    )
    kafka_hdfs_stream.read_from_kafka()
