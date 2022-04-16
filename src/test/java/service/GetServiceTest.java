package service;

import kafka.ReusableKafkaContainer;
import kafka.ReuseKafkaContainerExtension;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.KafkaContainer;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(ReuseKafkaContainerExtension.class)
class GetServiceTest {
    @ReusableKafkaContainer
    KafkaContainer kafka;


    @Test
    void getRecordsSize() {
        String bootstrapServers = kafka.getBootstrapServers();
        String topicName = "get-topic";

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaProducer producer = new KafkaProducer<>(properties);
        producer.send(new ProducerRecord(topicName, "hello"));
        producer.close();

        GetService getService = new GetService(bootstrapServers, topicName);
        int recordsSize = getService.getRecordsSize();

        assertEquals(1, recordsSize);
    }
}