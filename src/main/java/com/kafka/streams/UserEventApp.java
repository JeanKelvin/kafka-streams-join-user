package com.kafka.streams;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;

public class UserEventApp {

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-event-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStreamBuilder builder = new KStreamBuilder();

        // we get a global table out of Kafka. This table will be replicated on each Kafka Streams application
        // the key  of our globalTable is the user ID
        GlobalKTable<String, String> usersGlobalTable = builder.globalTable("user-table");

        // we get a stream of users purchases
        KStream<String, String> usersPurchases = builder.stream("user-purchases");

        // we want to enrich that streams
        KStream<String, String> userPurchasesEnrichedJoin =
                usersPurchases.join(usersGlobalTable,
                        (key, value) -> key, /* map from the (key, value) of this stream to the key of the GlobalTable */
                        (usersPurchase, userInfo) -> "Purchase=" + usersPurchase + ",UserInfo=[" + userInfo + "]"
                );

        userPurchasesEnrichedJoin.to("user-purchases-enriched-inner-join");

        // we wanto to enriche that stream using a left join
        KStream<String, String> userPurchasesEnrichedLeftJoin =
                usersPurchases.leftJoin(usersGlobalTable,
                        (key, value) -> key, /* map from the (key, value) of this stream to the keyof the GlobalKTable*/
                        (usersPurchase, userInfo) -> {
                            // as this is a left join, userInfo can be null
                            if (userInfo != null) {
                                return "Purchase=" + usersPurchase + ",UserInfo=[" + userInfo + "]";
                            } else {
                                return "Purchase=" + usersPurchase + ",UserInfo=null";
                            }
                        }
                );

        userPurchasesEnrichedLeftJoin.to("user-purchases-enriched-left-join");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.cleanUp();
        streams.start();

        // print the topology
        System.out.println(streams.toString());

        //shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
