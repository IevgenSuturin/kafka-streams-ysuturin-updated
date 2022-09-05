package com.ysuturin.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {

    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
//        1. Stream from Kafka  <null, "Kafka Kafka Streams">
        KStream<String, String> worldCountInput = builder.stream("word-count-input");
//        2. MapValues lowercase < null, "kafka kafka streams">
        KTable<String, Long> wordCounts = worldCountInput.mapValues(value -> value.toLowerCase())
//        3. FlatMapValues split by space <null, "kafka">, <null, "kafka">, <null, "streams">
                .flatMapValues(lowerCaseTextLine -> Arrays.asList(lowerCaseTextLine.split(" ")))
//        4. Select Key ot apply a key <"kafka", "kafka">, <"kafka", "kafka">, <"streams", "streams">
                .selectKey((ignoredKey, world) -> world)
//        5. GroupByKey before aggregation (<"kafka", "kafka">, <"kafka", "kafka">), (<"streams", "streams">)
                .groupByKey()
//        6. Count occurrences in each group <"kafka", 2>, <"streams", 1>
                .count(Named.as("Counts"));
//        7. To in order to write the results back to Kafka  - data point written to Kafka

        wordCounts.toStream()
                .to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }

    public static void main(String[] args) {
        Properties config = new Properties();

        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-application");
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        WordCountApp wordCountApp = new WordCountApp();

        KafkaStreams streams = new KafkaStreams(wordCountApp.createTopology(), config);
        streams.start();
//     Printing topology
        System.out.println(streams.toString());
//     Add shutdown hook to stop the Kafka Streams thread
//     You  can optionally provide a timeout to close
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
