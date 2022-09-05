package com.ysuturin.kafka.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BankTransactionProducerTest {

    @Test
    public void testNewRandomTransactions() {
        ProducerRecord<String, String> record = BankTransactionProducer.newRandomTransaction("john");

        String key = record.key();
        String value = record.value();

        assertEquals(key, "john");

        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode node = mapper.readTree(value);
            assertEquals(node.get("name").asText(), "john");
            assertTrue("Amount should be less than 100", node.get("amount").asInt() < 100);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        System.out.println(value);
    }

}
