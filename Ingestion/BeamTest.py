import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.io.hadoopfilesystem import HadoopFileSystem
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.external import BeamJarExpansionService

options = PipelineOptions(
    topic="bankretail",
    bootstrap_servers="localhost:9092",
    output_dir="hdfs://beam_output_v1/bank_retail",
    save_main_session=True,
    streaming=True
)

expansion_service = BeamJarExpansionService("sdks:java:io:expansion-service:shadowJar")

with beam.Pipeline(options=options) as p:
    # with beam.Pipeline(options=self.pipeline_options) as p:
    (p | ReadFromKafka(
                consumer_config={"bootstrap.servers": "localhost:9092"},
                topics=["bankretail"],
                with_metadata=False
            )
     | beam.Map(print)
     )
