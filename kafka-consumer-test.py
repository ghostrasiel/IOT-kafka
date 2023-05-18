from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from urilb.get_redish import RedishOpPut

consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'])
# data = []

topics = '^test-IOT.*'
consumer.subscribe(pattern=topics)
# print(consumer.partitions_for_topic('test'))
print(consumer.topics())
print(consumer.subscription())
print(consumer.assignment())
print(consumer.beginning_offsets(consumer.assignment()))
# consumer.seek(TopicPartition(topic='test', partition=0) , 100 ) ##設定偏移量
redish = RedishOpPut()
for msg in consumer:
    data = eval(msg.value)
    result = redish.send_redish_data(data=data,
                                     key='test_IOT',
                                     find=data["machine"]
                                     )
    if result == False:
        break
    print(data)
