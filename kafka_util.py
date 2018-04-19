# -*- coding: utf-8 -*-
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json


class Kafka_producer():
    '''
    desc: kafka生产者
    params:
        kafka_conf_list  kafka的ip和端口配置，集群方式
        kafkatopic: kafka的topic主题
    attention： 目前测试发现，发送的内容为字节，其它内容会报错
    '''
    def __init__(self, kafka_conf, port=None):
        self.producer = KafkaProducer(bootstrap_servers = '{}'.format(kafka_conf))

    def sendjsondata(self, kafkatopic, params):
        try:
            if isinstance(params,str):
                parmas_message = params
            else:
                parmas_message = json.dumps(params)
            self.producer.send(kafkatopic, parmas_message.encode('utf-8'))
            self.producer.flush()
        except:
            raise KafkaError("send message to kafka error")
    
    def close(self):
        self.producer.close(60)
    


class Kafka_consumer():
    '''
    desc: Kafka的消费模块
    params: 
        
        kafkatopic:  主题
        groupid: 用户组
    '''

    def __init__(self, kafka_conf, kafkatopic, groupid='test'):
        self.consumer = KafkaConsumer(kafkatopic,
                                      bootstrap_servers = '{}'.format(kafka_conf),
                                      auto_offset_reset='smallest' #earliest
                                      )
        

    def consume_data(self):
        results = []
        try:
            for message in self.consumer:
                message = json.loads(message.value)
                yield message
        except KeyboardInterrupt as e:
            raise KeyboardInterrupt('comsume kafka message error')
        
    def close(self):
        self.consumer.close()


def send_message_test(kafka_conf,topic,msg):
    producer = Kafka_producer(kafka_conf)
    producer.sendjsondata(topic,msg)
    return

def consume_message_test(kafka_conf,topic,group='test'):
    consumer = Kafka_consumer(kafka_conf,topic)
    results = consumer.consume_data()
    return results


if __name__ == '__main__':
    env = 'localhost'
    if env == 'localhost':
        kafka_conf = "10.50.1.197:19092,10.50.1.202:19092,10.50.1.198:19092"
    else:
        kafka_conf = '114.115.148.216:19092,114.115.148.20:19092,114.115.148.52:19092'
    topic = 'kcytest'
    msg = {'name':'荣之联','low':8,'id':1}
    send_message_test(kafka_conf, topic, msg)
    ss = consume_message_test(kafka_conf, topic)
    for s in ss:
        print (s)
    