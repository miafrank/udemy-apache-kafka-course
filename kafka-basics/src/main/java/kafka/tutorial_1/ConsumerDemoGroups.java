package kafka.tutorial_1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


public class ConsumerDemoGroups {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class.getName());

        String BOOTSTRAP_SERVER = "127.0.0.1:9092";
        String GROUP_ID = "my-fifth-application";
        String FIRST_TOPIC = "first_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        // producer takes a string and serializes to bytes. consumer takes bytes and deserializes.
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create consume
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        // Subscribe consumer to our topic(s)
        consumer.subscribe(Collections.singleton(FIRST_TOPIC));
        // Poll for new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in 2.0.0

            for (ConsumerRecord<String, String> record : records) {
                logger.info(("Key: " + record.key() + ", Value: " + record.value()));
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }
    }
}
