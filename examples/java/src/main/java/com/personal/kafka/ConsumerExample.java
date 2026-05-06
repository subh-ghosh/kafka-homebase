package com.personal.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerExample {
  public static void main(String[] args) {
    String bootstrap = System.getenv("KAFKA_BOOTSTRAP");
    String topic = System.getenv().getOrDefault("KAFKA_TOPIC", "app1.events");
    String username = System.getenv("KAFKA_USERNAME");
    String password = System.getenv("KAFKA_PASSWORD");
    String truststore = System.getenv().getOrDefault("KAFKA_TRUSTSTORE", "./kafka.truststore.p12");
    String truststorePass = System.getenv("KAFKA_TRUSTSTORE_PASSWORD");
    String group = System.getenv().getOrDefault("KAFKA_GROUP", "app1");

    if (bootstrap == null || username == null || password == null || truststorePass == null) {
      throw new IllegalArgumentException("Missing env: KAFKA_BOOTSTRAP, KAFKA_USERNAME, KAFKA_PASSWORD, KAFKA_TRUSTSTORE_PASSWORD");
    }

    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrap);
    props.put("group.id", group);
    props.put("auto.offset.reset", "earliest");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    props.put("security.protocol", "SASL_SSL");
    props.put("sasl.mechanism", "SCRAM-SHA-512");
    props.put("sasl.jaas.config",
        "org.apache.kafka.common.security.scram.ScramLoginModule required " +
            "username=\"" + username + "\" password=\"" + password + "\";");

    props.put("ssl.truststore.type", "PKCS12");
    props.put("ssl.truststore.location", truststore);
    props.put("ssl.truststore.password", truststorePass);

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(List.of(topic));
      System.out.println("consuming " + topic + "...");

      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        records.forEach(r -> System.out.printf("%s[%d]@%d: %s%n", r.topic(), r.partition(), r.offset(), r.value()));
      }
    }
  }
}
