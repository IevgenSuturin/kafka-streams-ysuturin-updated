package com.ysuturin.kafka.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Instant;
import java.util.Properties;

public class BankBalanceExactlyOnceApp {

    public static void main(String[] args) {
        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // we disabled the cash to demonstrate all the "steps" involved in the transformation
        // not recommended in prod
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        // Exactly once processing
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        // Json Serde
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, JsonNode> bankTransactions =
                builder.stream("bank-transactions", Consumed.with(Serdes.String(), jsonSerde));

        //create the initial json object for balance
        JsonNode initialBalance = JsonNodeFactory.instance.objectNode()
        .put("count", 0)
        .put("balance", "0")
        .put("time", Instant.ofEpochMilli(0L).toString());

        KTable<String, JsonNode> bankBalance = bankTransactions
                .groupByKey(Grouped.with(Serdes.String(), jsonSerde))
                .aggregate(
                        () -> initialBalance,
                        (key, transaction, balance) -> newBalance(transaction, balance),
                        Named.as("bank-balance-agg"),
                        Materialized.with(Serdes.String(), jsonSerde)
                );

        bankBalance.toStream()
                .to("bank-balance-exactly-once", Produced.with(Serdes.String(), jsonSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        streams.start();

        System.out.println(streams);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    private static JsonNode newBalance(JsonNode transaction, JsonNode balance) {
        // create a new balance json Object
        ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
        newBalance.put("count", balance.get("count").asInt() + 1);
        newBalance.put("balance", balance.get("balance").asInt() + transaction.get("amount").asInt());

        Long balanceEpoch = Instant.parse(balance.get("time").asText()).toEpochMilli();
        Long transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli();
        Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));
        newBalance.put("time", newBalanceInstant.toString());
        return newBalance;
    }
}
