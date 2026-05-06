package com.personal.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerExample {
  public static void main(String[] args) {
    String bootstrap = System.getenv("KAFKA_BOOTSTRAP");
    String topic = System.getenv().getOrDefault("KAFKA_TOPIC", "app1.events");
    String username = System.getenv("KAFKA_USERNAME");
    String password = System.getenv("KAFKA_PASSWORD");
    String truststore = System.getenv().getOrDefault("KAFKA_TRUSTSTORE", "./kafka.truststore.p12");
    String truststorePass = System.getenv("KAFKA_TRUSTSTORE_PASSWORD");

    if (bootstrap == null || username == null || password == null || truststorePass == null) {
      throw new IllegalArgumentException("Missing env: KAFKA_BOOTSTRAP, KAFKA_USERNAME, KAFKA_PASSWORD, KAFKA_TRUSTSTORE_PASSWORD");
    }

    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrap);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    props.put("security.protocol", "SASL_SSL");
    props.put("sasl.mechanism", "SCRAM-SHA-512");
    props.put("sasl.jaas.config",
        "org.apache.kafka.common.security.scram.ScramLoginModule required " +
            "username=\"" + username + "\" password=\"" + password + "\";");

    props.put("ssl.truststore.type", "PKCS12");
    props.put("ssl.truststore.location", truststore);
    props.put("ssl.truststore.password", truststorePass);

    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
      producer.send(new ProducerRecord<>(topic, "{\"hello\":\"world\"}"));
      producer.flush();
      System.out.println("sent");
    }
  }
}
