package com.LoopLink.pipeHub;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Slf4j
public class NormalTest {
    // public static void main(String[] args) {
    //     String servers = "localhost:9092,null,kafka-01:9092";
    //
    //     List<String> list = Arrays.asList(StringUtils.split(servers, ","));
    //
    //     System.out.println(list);
    // }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.27.199.161:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>("send-center-sms", "key1", "{\"id\":1,\"name\":\"test message\"}");
            producer.send(record, (metadata, e) -> {
                if (e != null) {
                    log.error(e.getMessage(), e);
                } else {
                    System.out.println("Message sent to topic " + metadata.topic() + " partition " + metadata.partition());
                }
            });
        }
    }
}
