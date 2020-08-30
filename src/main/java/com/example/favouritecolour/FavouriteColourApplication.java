package com.example.favouritecolour;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;
import java.util.Properties;

@Slf4j
@SpringBootApplication
public class FavouriteColourApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(FavouriteColourApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        Properties config = new Properties();
        //Task 0: Assign your team name to "teamName" variable
        String teamName = "";
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, String.format("favourite-colour-%s", teamName));
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "willy:9292");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:8081");
        config.put(ConsumerConfig.DEFAULT_ISOLATION_LEVEL, true);

        StreamsBuilder builder = new StreamsBuilder();

        //Task 1: send filtered colours as "name" key and "colour" value in KStream to topic: {teamname}-filtered-colours

            // 1 - create Kstream based on the input topic "favourite-colour-input"


            // 2 - make sure that a comma is contained in the value as we will split on it

            // 3 - select a key that will be the user name (lowercase for safety)

            // 4 - get the colour from the value (lowercase for safety)

            // 5 - log out what key and value you have so far

            // 6 - filter undesired colours (only green, purple and yellow colours should be sent)

            // 7 - send filtered kstream to {teamname}-filtered-colours topic, don't forget the serializers


        //Task 2: count the filtered colours and send them to topic {teamname}-favourite-colour-output

            // Step 1 - read that {teamname}-filtered-colours topic as a KTable so that updates are read correctly


            // Step 2 - count the occurrences of colours

                    // 1 - group by colour within the KTable

                    // 2 - count colours that are grouped, map the values to string to make the visualization good

                    // 3 - output the results to a Kafka topic {teamname}-favourite-colour-output - don't forget the serializers



        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        // only do this in dev - not in prod
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
