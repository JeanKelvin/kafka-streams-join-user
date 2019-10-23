package com.kafka.streams;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

// Run the Kafka Streams applications before running the producer.
public class UserDataProducer {

    public static void main(String[] args) throws InterruptedException, ExecutionException {

        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all"); //strongest producing guarantee
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");

        // leverage idempotent producer from kafka 0.11
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); //ensure we don't push duplicates


        Producer<String, String> producer = new KafkaProducer<>(properties);

        // 1 - we create a new user, the we send some data to kafka
        System.out.println("\nExample 1 - new user\n");
        producer.send(userRecord("john", "First=Jonh, Last=Doe,Email=john.doe@gmail.com")).get();
        producer.send(purchaseRecord("john", "Apples and Bananas (1)")).get();

        Thread.sleep(10000);


        // 2 - we receiver user purchase, but it doesn't exist in Kafka
        System.out.println("\nExample 2 - not existing user\n");
        producer.send(purchaseRecord("bob", "Kafka course (2)")).get();

        Thread.sleep(10000);


        // 3 - we update user "john", and send a new transaction
        System.out.println("\nExample 3 - update to user\n");
        producer.send(userRecord("john", "First=Jonh, Last=Doe,Email=john.doe@gmail.com")).get();
        producer.send(purchaseRecord("john", "Oranges (3)")).get();

        Thread.sleep(10000);


        // 4 - we send a user purchases for ana, but it exists in Kafka later
        System.out.println("\nExample 4 - not existing user then user\n");
        producer.send(purchaseRecord("ana", "Computer (4)")).get();
        producer.send(userRecord("ana", "First=Ana, Last=Robben,Email=ana.robben@gmail.com")).get();
        producer.send(purchaseRecord("ana", "Books (4)")).get();
        producer.send(userRecord("ana", null)).get(); //delete for cleanup

        Thread.sleep(10000);

        // 5 - we create a user, but it gets deleted before any purchase comes through
        System.out.println("\nExample 5 - user then delete then data\n");
        producer.send(userRecord("alice", "First=Alice")).get();
        producer.send(userRecord("alice", null)).get(); // that's the delete record
        producer.send(purchaseRecord("alice", "Apache Kafka Series (5)")).get();


        System.out.println("End of demo");
        producer.close();
    }

    private static ProducerRecord<String, String> userRecord(String key, String value) {
        return new ProducerRecord<>("user-table", key, value);
    }

    private static ProducerRecord<String, String> purchaseRecord(String key, String value) {
        return new ProducerRecord<>("user-purchases", key, value);
    }


}
