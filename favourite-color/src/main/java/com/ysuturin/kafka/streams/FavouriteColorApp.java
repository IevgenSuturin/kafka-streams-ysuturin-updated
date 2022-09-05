package com.ysuturin.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class FavouriteColorApp {

    public static void main(String[] args) {
        Properties config = new Properties();

        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-color-application");
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        config.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        StreamsBuilder builder = new StreamsBuilder();

        // Step 1: We create the topic of users keys to colours
        KStream<String, String> textLines = builder.stream("favourite-colour-input");

        KStream<String, String> userToColours = textLines
                .filter((key, value) -> value.contains(","))
                .selectKey((key, value) -> value.split(",")[0].toLowerCase())
                .mapValues(value -> value.split(",")[1].toLowerCase())
                .filter((user, colour) -> Arrays.asList("green", "blue", "red").contains(colour));

        userToColours.to("user-keys-and-colours");

        // step 2 - we read that topic as a KTable so that updates are read correctly
        KTable<String, String> userAndColorsTable = builder.table("user-keys-and-colours");

        // step 3 - we count the occurrences of colours
        KTable<String, Long> favoriteColors = userAndColorsTable
                // 5 - we group by colour within the KTable
                .groupBy((user, colour) -> new KeyValue<>(colour, colour))
                .count(Named.as("CountsByColors"));

        // 6 - we output the results to a Kafka Topic - don't forget the serializers
        favoriteColors.toStream()
                .to("favourite-colour-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        //only do this in dev - not in prod
        streams.cleanUp();
        streams.start();

        System.out.println(streams);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
