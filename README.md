# Flink Kafka Docker App

This is a simple docker-compose app that runs a Flink job that consumes from a Kafka/Kinesis topic and produces to file sink.

Documentation : 
- https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/connectors/table/kinesis/
- https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/datastream/fault-tolerance/state/

## Prerequisites

- Docker
- Python 3.9

## Usage

To install the dependencies, run:

```bash

poetry install

```

To build the image for the first time, run:

```bash

docker-compose up -d

```

To refresh the images, run:

```bash

docker-compose up -d --build

```

To log into the container, run:

```bash

docker exec -it app /bin/bash

```

To run the Kafka producer, run:

```bash

python3 kafka_producer.py

```

To run the Flink consumer, run locally:

```bash

/flink/bin/flink run -py /taskscripts/flink_kafka_consumer_datastream.py --jobmanager jobmanager:8081 --target local

```

To run the Flink consumer, run on a cluster:

```bash

/flink/bin/flink run -py /taskscripts/flink_kafka_consumer_datastream.py --jobmanager jobmanager:8081

```

To run the Flink producer, run locally:

```bash

/flink/bin/flink run -py /taskscripts/flink_kafka_producer_datastream.py --jobmanager jobmanager:8081 --target local

```

To run the Flink consumer with Table API, run locally:

```bash

/flink/bin/flink run -py /taskscripts/flink_kinesis_consumer_table.py --jobmanager jobmanager:8081 --target local

```

To run the Flink consumer with Datastream API on EMR cluster with `command-runner.jar`, run:

```bash

bash -c "aws s3 cp s3://upwork-usw2-transient-lfs-emr/streaming-poc/flink-streaming/ /home/hadoop/ --recursive && pip install /home/hadoop/flink_with_python-0.1.0.tar.gz && flink run -m yarn-cluster -py /home/hadoop/flink_kinesis_consumer_datastream.py"

```