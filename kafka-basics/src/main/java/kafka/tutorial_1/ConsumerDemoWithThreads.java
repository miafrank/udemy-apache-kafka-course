package kafka.tutorial_1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class ConsumerDemoWithThreads {
    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();
    }

    private ConsumerDemoWithThreads() {
    }

    private void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());
        String BOOTSTRAP_SERVER = "127.0.0.1:9092";
        String GROUP_ID = "my-abc-application";
        String FIRST_TOPIC = "first_topic";

        // Latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);
        logger.info("Creating the consumer thread");

        // Create the consumer runnable
        ConsumerRunnable myConsumerRunnable = new ConsumerRunnable(
                BOOTSTRAP_SERVER,
                GROUP_ID,
                FIRST_TOPIC,
                latch);

        // Start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();
        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> {
                    logger.info("Caught shutdown hook");
                    ((ConsumerRunnable) myConsumerRunnable).shutDown();
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    logger.info("Application has exited");
                }

        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }

    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private final KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(String bootstrapServers,
                                String groupId,
                                String topic,
                                CountDownLatch latch) {
            this.latch = latch;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            // producer takes a string and serializes to bytes. consumer takes bytes and deserializes.
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            consumer = new KafkaConsumer<String, String>(properties);
            // Subscribe consumer to our topic(s)
            consumer.subscribe(Collections.singleton(topic));
            // Poll for new data
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in 2.0.0

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info(("Key: " + record.key() + ", Value: " + record.value()));
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                // tell main code that we are done with the consumer
                latch.countDown();
            }
        }

        public void shutDown() {
            // method to interrupt consumer.poll()
            // it will throw exception WakeUpException
            consumer.wakeup();
        }
    }
}
