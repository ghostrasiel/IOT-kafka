from urilb import spark_lib

schema_value = {
    "machine": str,
    "temperature": int,
    "humidity": int
}
kafka_op_data = {
    "path": "test/output/test-IOT",
    "schema_vlaue": schema_value,
    "projuct_name": "test-IOT"
}
spark = spark_lib.SparkOpPut(projuct_name=kafka_op_data["projuct_name"])
spark.kafka_streaming(path=kafka_op_data["path"],
                      schema_value=kafka_op_data["schema_vlaue"]
                      )
