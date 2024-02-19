from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.window import Tumble


def perform_tumbling_window_aggregation(table_env, input_table_name):
    # use SQL Table in the Table API
    input_table = table_env.from_path(input_table_name)

    tumbling_window_table = (
        input_table.window(
            Tumble.over("10.seconds").on("odeskdb_openings").alias("ten_second_window")
        )
        .group_by("ticker, ten_second_window")
        .select("ticker, price.min as price, to_string(ten_second_window.end) as event_time")
    )

    return tumbling_window_table


def main():
    # Initialize Table Environment
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    # t_env.get_config().set("python.execution-mode", "thread") # for jvm and process for python
    t_env.get_config().set("pipeline.jars", "file:///home/hadoop/flink-sql-connector-kinesis-4.2.0-1.18.jar")
    source_ddl = """
            CREATE TABLE KinesisTable(
                odeskdb_openings ROW<opening_title VARCHAR, job_description VARCHAR>
            ) WITH (
              'connector' = 'kinesis',
              'stream' = 'agora-prod-ds-live-data-relay',
              'aws.region' = 'us-west-2',
              'aws.endpoint' = 'https://kinesis.us-west-2.amazonaws.com',
              'scan.stream.initpos' = 'LATEST',
              'format' = 'json',
              'aws.trust.all.certificates' = 'true',
              'aws.credentials.basic.accesskeyid'= '',
              'aws.credentials.basic.secretkey'= ''
            )"""

    sink_ddl = """
            CREATE TABLE sink_table(
                odeskdb_openings ROW<opening_title VARCHAR, job_description VARCHAR>
            ) WITH (
              'connector' = 'filesystem',
              'path' = '/sink/kinesis/table',
              'format' = 'json'
            )"""
    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_ddl)
    t_env.sql_query("SELECT odeskdb_openings FROM KinesisTable").execute_insert("sink_table").wait()


if __name__ == "__main__":
    main()
