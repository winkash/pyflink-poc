from pyflink.common import Encoder
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy, BucketAssigner
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
        # 'aws.iam.role': 'arn:aws:iam::123456789012:role/MyKinesisRole', -- for cluster
    }

    # Define the Kinesis source
    kinesis_source = FlinkKinesisConsumer(
        'agora-prod-ds-live-data-relay',  # Your Kinesis stream name
        SimpleStringSchema(),  # Deserializer for the data in the stream
        properties)

    # Add the source to the execution environment
    data_stream = env.add_source(kinesis_source)

    output_path = f's3://upwork-usw2-transient-lfs-emr/streaming-poc/flink-streaming/streams/'

    file_sink = FileSink.for_row_format(
        base_path=output_path,
        encoder=Encoder.simple_string_encoder()) \
        .with_output_file_config(OutputFileConfig.builder().build()) \
        .with_rolling_policy(RollingPolicy.default_rolling_policy()) \
        .build()

    # Sink the data stream to the FileSink
    data_stream.sink_to(file_sink)

    # Execute the Flink job
    env.execute("Flink Kinesis Consumer Example")


if __name__ == "__main__":
    main()
