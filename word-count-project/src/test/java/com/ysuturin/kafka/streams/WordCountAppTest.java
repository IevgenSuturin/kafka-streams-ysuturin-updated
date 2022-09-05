package com.ysuturin.kafka.streams;

import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WordCountAppTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;
    private StringSerializer stringSerializer = new StringSerializer();

    @Before
    public void setUpTopologyTestDriver() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        Topology topology = new WordCountApp().createTopology();
        testDriver = new TopologyTestDriver(topology, config);
        inputTopic = testDriver.createInputTopic("word-count-input", stringSerializer, stringSerializer);
        outputTopic = testDriver.createOutputTopic("word-count-output", new StringDeserializer(), new LongDeserializer());
    }

    @After
    public void closeTestDriver() {
        testDriver.close();
    }

    @Test
    public void dummyTest() {
        String dummy = "Du" + "mmy";
        assertEquals(dummy, "Dummy");
    }

    @Test
    public void  testIfCountsAreCorrect() {
        String firstExample = "testing Kafka Streams";
        pushNewInputRecord(firstExample);
        assertEquals(readOutPut().key, "testing");
        assertEquals(readOutPut().key, "kafka");
        assertEquals(readOutPut().key, "streams");
        assertTrue(outputTopic.isEmpty());
    }

    private void pushNewInputRecord(String value) {
        inputTopic.pipeInput(null, value);
    }

    private KeyValue<String, Long> readOutPut() {
        return outputTopic.readKeyValue();
    }
}
