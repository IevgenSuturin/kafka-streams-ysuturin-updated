package com.ysuturin.kafka.streams;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class BankTransactionProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();

        //kafka bootstrap server
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //producer acks
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        //leverage idempotent producer from Kafka 0.11
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); //ensure we don't push duplicates

        Producer<String, String> producer = new KafkaProducer<>(properties);

        int i=0;
        while(true) {
            System.out.println("Producing batch: " + i);
            try {
                producer.send(newRandomTransaction("john"));
                Thread.sleep(100);
                producer.send(newRandomTransaction("stefan"));
                Thread.sleep(100);
                producer.send(newRandomTransaction("alise"));
                Thread.sleep(100);
            } catch (InterruptedException e) {
                break;
            }
            i++;
        }
    }

    public static ProducerRecord<String, String> newRandomTransaction(String name){
        //create an empty json node
        ObjectNode transaction = JsonNodeFactory.instance.objectNode();

        // { "amount" : 46 } 46 is random number between 0 and 100 excluded
        Integer amount = ThreadLocalRandom.current().nextInt(0, 100);

        // Instant.now() is to get the current time using Java
        Instant now = Instant.now();

        // we write the data to the Json document
        transaction.put("name", name);
        transaction.put("amount", amount);
        transaction.put("time", now.toString());
        return new ProducerRecord<>("bank-transactions", name, transaction.toString());
    }
}
