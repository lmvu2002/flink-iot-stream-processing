
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def main():
    env = StreamExecutionEnvironment.get_execution_environment() # Get environment variables
    settings = EnvironmentSettings.in_streaming_mode() # Change setting to Streaming mode
    tenv = StreamTableEnvironment.create(env, settings) # Apply to tenv

    env.add_jars("file:///C:\\flink\\lib\\flink-sql-connector-kafka-3.4.0-1.20.jar")

    src_ddl = """
        CREATE TABLE sensor_readings (
            device_id VARCHAR,
            co DOUBLE,
            humidity DOUBLE,
            motion BOOLEAN,
            temp DOUBLE,
            ampere_hour DOUBLE,
            ts BIGINT,
            event_time AS TO_TIMESTAMP_LTZ(ts, 3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'new-topic',
            'properties.bootstrap.servers' = '172.29.154.143:9093',
            'properties.group.id' = 'device.tumbling.w.sql',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        )
    """
    print("START CREATING TABLE")
    tenv.execute_sql(src_ddl) # Execute the query to create reading stream table
    print("DONE")
    #sensor_readings_tab = tenv.from_path('sensor_readings')

    # Process a Tumbling Window Aggregate Calculation of Ampere-Hour
    # For every 30 seconds non-overlapping window
    # Calculate the total charge consumed grouped by device
    tumbling_w_sql = """
            SELECT
                device_id,
                TUMBLE_START(event_time, INTERVAL '30' SECONDS) AS window_start,
                TUMBLE_END(event_time, INTERVAL '30' SECONDS) AS window_end,
                ROUND(SUM(COALESCE(ampere_hour, 0)), 2) AS charge_consumed,
                ROUND(AVG(temp),2) as average_temperature,
                ROUND(AVG(humidity),2) as average_humidity
            FROM sensor_readings
            WHERE temp IS NOT NULL
            GROUP BY
                TUMBLE(event_time, INTERVAL '30' SECONDS),
                device_id
        """
    print("START QUERYING")
    tumbling_w = tenv.sql_query(tumbling_w_sql) # Execute the query to get data
    tumbling_w.print_schema() # Print the schema of the query
    print("DONE")

    sink_ddl = """
            CREATE TABLE devicecharge (
                device_id VARCHAR,
                window_start TIMESTAMP(3),
                window_end TIMESTAMP(3),
                charge_consumed DOUBLE,
                average_temperature DOUBLE,
                average_humidity DOUBLE
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'new-topic2',
                'properties.bootstrap.servers' = '172.29.154.143:9093',
                'scan.startup.mode' = 'earliest-offset',
                'properties.auto.offset.reset' = 'earliest',
                'format' = 'json'
            )
        """
    print("Start execute")
    tenv.execute_sql(sink_ddl) # Create write table
    print('START INSERTING')
    tumbling_w.execute_insert('devicecharge').wait() # Insert the data from last query to this write table

if __name__ == '__main__':
    main()