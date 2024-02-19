import os

from pyflink.common import Encoder
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.restart_strategy import RestartStrategies


def kafka_source_to_file_sink():
    """
    A Flink job that consumes messages from a Kafka topic, counts the number of characters
    in each message, and writes the counts to a file on disk.
    """
    # Create a StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()

    env.set_restart_strategy(RestartStrategies.fixed_delay_restart(
        1,  # max number of restart attempts
        10000  # delay between attempts
    ))

    # the kafka/sql jar is used here as it's a fat jar and could avoid dependency issues
    env.add_jars("file:///home/hadoop/flink-sql-connector-kafka-3.0.1-1.18.jar")

    # Define the Kafka source
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(os.environ["KAFKA_BROKER"]) \
        .set_topics(os.environ["KAFKA_TOPIC"]) \
        .set_group_id("flink_consumer_group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # Apply a WatermarkStrategy for event-time processing
    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps()

    # Add the Kafka source to the environment
    ds = env.from_source(source=kafka_source,
                         watermark_strategy=watermark_strategy,
                         source_name="Kafka Source")

    # Process the data: map each string to its length
    processed_ds = ds.map(lambda x: (x, len(x)), output_type=Types.TUPLE([Types.STRING(), Types.INT()]))

    # Define the output path
    output_path = os.path.join(os.environ.get("SINK_DIR", "/sink"), "message.log")

    # Define the File sink
    file_sink = FileSink.for_row_format(
        base_path=output_path,
        encoder=Encoder.simple_string_encoder()) \
        .with_output_file_config(OutputFileConfig.builder().build()) \
        .with_rolling_policy(RollingPolicy.default_rolling_policy()) \
        .build()

    # Sink the processed data to the file
    processed_ds.sink_to(file_sink)

    # Execute the Flink job
    env.execute("Kafka Source to File Sink")


if __name__ == "__main__":
    kafka_source_to_file_sink()
