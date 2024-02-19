import os

from pyflink.common import Encoder
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy
from pyflink.datastream.connectors.kinesis import FlinkKinesisConsumer


def main():
    # Initialize the StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()

    env.add_jars("file:///home/hadoop/flink-sql-connector-kinesis-4.2.0-1.18.jar")

    # Define properties for the Kinesis consumer
    properties = {
        "aws.region": "us-west-2",  # Specify your AWS region
        "flink.stream.initpos": "LATEST",  # Start reading at
        'aws.credentials.provider.basic.accesskeyid': '',
        'aws.credentials.provider.basic.secretkey': '',
    }

    # Define the Kinesis source
    kinesis_source = FlinkKinesisConsumer(
        'agora-prod-ds-live-data-relay',  # Your Kinesis stream name
        SimpleStringSchema(),  # Deserializer for the data in the stream
        properties)

    # Add the source to the execution environment
    data_stream = env.add_source(kinesis_source)

    data_stream.map(lambda x: "Received: " + x, output_type=Types.STRING())

    output_path = os.path.join(os.environ.get("SINK_DIR", "/tmp/sink"), "kinesis/datastream")

    file_sink = FileSink.for_row_format(
        base_path=output_path,
        encoder=Encoder.simple_string_encoder()) \
        .with_output_file_config(OutputFileConfig.builder().build()) \
        .with_rolling_policy(RollingPolicy.default_rolling_policy()) \
        .build()

    # Sink the processed data to the file
    data_stream.sink_to(file_sink)

    # Execute the Flink job
    env.execute("Flink Kinesis Consumer Example")


if __name__ == "__main__":
    main()
