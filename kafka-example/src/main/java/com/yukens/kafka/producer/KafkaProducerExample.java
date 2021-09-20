package com.yukens.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * <p>description</p >
 *
 * @author Trump
 * @version 1.0
 * @date 2021/09/20 13:18
 */
public class KafkaProducerExample {

  public static final String ORDER_TOPIC = "order_topic";

  public static void main(String[] args) throws InterruptedException, ExecutionException {
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, "0");
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
    properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    KafkaProducer<String, String> kafkaProducer = new KafkaProducer(properties);
    for (int i = 0; i < 1000; i++) {
      ProducerRecord<String, String> record = new ProducerRecord<String, String>(ORDER_TOPIC, System.currentTimeMillis() + "");
//      Future<RecordMetadata> send = kafkaProducer.send(record);
      kafkaProducer.send(record, (recordMetadata, e) -> {
        int partition = recordMetadata.partition();
        long offset = recordMetadata.offset();
        System.out.println(String.format("partition is %d,offset is %d", partition, offset));
      });
//      RecordMetadata metadata = send.get();
//      int partition = metadata.partition();
//      long offset = metadata.offset();
//      System.out.println(String.format("partition is %d,offset is %d", partition, offset));

      TimeUnit.MILLISECONDS.sleep(1000);
    }
    kafkaProducer.close();
  }
}
