"""取得Spark 連線."""

import os
from datetime import datetime

import findspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


os.environ['PYSPARK_SUBMIT_ARGS'] = """
--packages \
org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,\
org.apache.spark:spark-avro_2.12:3.4.0 \
pyspark-shell
"""
findspark.init()


class SparkOpPut():
    """主要為Spark取得資料&寫入Hadoop資料."""

    def __init__(self, projuct_name: str):
        """載入基本設定包括Spark & Kafka."""
        self.projuct_name = projuct_name
        self.spark = SparkSession.builder \
            .appName(f'spark-{self.projuct_name}') \
            .getOrCreate()

        self.spark_server = 'localhost:9000'
        self.kafka_server = 'localhost:9092'

    def __kafka_option_stream(self):
        check_points_path = f'/tmp/checkpoints/{self.projuct_name}'
        kafka_Stream = self.spark.readStream.format('kafka') \
            .option('kafka.bootstrap.servers', self.kafka_server) \
            .option('subscribePattern', f'{self.projuct_name}.*') \
            .option('startingOffsets', 'earliest') \
            .option('checkpointLocation', check_points_path) \
            .load()
        return kafka_Stream

    def __write_parquet_path(self, batch_df, batch_id, hadoop_path: str):
        # 根据当前时间构造文件名
        file_name = datetime.now().strftime('%y-%m-%d')
        write_path = f'{hadoop_path}/{file_name}/'
        # 写入Parquet格式的数据
        batch_df.write.mode('append').parquet(write_path)

    def __option_schema(self, schema_value: dict):
        schema_list = []
        for schema in schema_value.items():
            if schema[1] == str:
                schema_list.append(StructField(schema[0], StringType()))
            elif schema[1] == int:
                schema_list.append(StructField(schema[0], IntegerType()))
        return StructType(schema_list)

    def kafka_streaming(self, path: str, schema_value: dict):
        """接收Kafka Streaming Data 資料."""
        schema = self.__option_schema(schema_value=schema_value)
        kafka_stream = self.__kafka_option_stream()

        # 定義所有欄位名稱的清單
        all_columns = list(schema_value.keys())
        # 根據清單內容動態產生需要的選擇列
        select_cols = [col('parsed_value.' + col_name).alias(col_name)
                       for col_name in all_columns
                       if col_name in schema.fieldNames()
                       ]
        select_cols.append(col('timestamp'))

        schema_df = kafka_stream.selectExpr('CAST(value AS STRING)',
                                            'timestamp') \
            .select(from_json('value', schema).alias('parsed_value'),
                    'timestamp') \
            .select(*select_cols)

        hadoop_path = f'hdfs://{self.spark_server}/{path}'
        check_points_path = f'/tmp/checkpoints/{self.projuct_name}'
        write_hadoop = schema_df.writeStream \
            .format('parquet') \
            .foreachBatch(lambda batch_df, batch_id:
                          self.__write_parquet_path(batch_df,
                                                    batch_id,
                                                    hadoop_path)
                          ) \
            .option('checkpointLocation', check_points_path) \
            .trigger(processingTime='1 hour') \
            .start()

        write_hadoop.awaitTermination()
