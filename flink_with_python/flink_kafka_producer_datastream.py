import os

from pyflink.common import Types, Row
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, TimeCharacteristic
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowSerializationSchema


def create_kafka_sink(topic, bootstrap_servers):
    """
    Create a Kafka sink for sending messages to a Kafka topic.
    """

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(
                JsonRowSerializationSchema.builder().with_type_info(value_type_info).build()
            )
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    return sink


if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)
    env.add_jars("file:///home/hadoop/flink-sql-connector-kafka-3.0.1-1.18.jar")

    value_type_info = Types.ROW_NAMED(
        field_names=["data"],
        field_types=[Types.STRING()],
    )

    source_stream = env.from_collection(
        collection=[
            [
                (("user1", "gold"), ("user2", "gold"), ("user5", "gold")),
                (("user3", "gold"), ("user4", "gold"), ("user6", "gold")),
            ]
        ]
    )

    # Retrieve Kafka configuration from environment variables
    bootstrap_servers = os.getenv("KAFKA_BROKER")  # Kafka broker configuration
    topic = os.getenv("KAFKA_TOPIC")  # Target Kafka topic

    # Create Kafka sink with the topic and broker information
    kafka_sink: KafkaSink = create_kafka_sink(topic, bootstrap_servers)

    source_stream.map(lambda e: Row(data=str(e)), output_type=value_type_info).sink_to(kafka_sink).name(
        "kafka sink").uid("kafka sink")

    env.execute("Flink kafka producer")
