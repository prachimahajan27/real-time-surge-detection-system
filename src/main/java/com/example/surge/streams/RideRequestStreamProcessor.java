package com.example.surge.streams;

import com.example.surge.model.RideRequest;
import com.example.surge.model.SurgeEvent;

import com.example.surge.model.SurgeState;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;

import org.apache.kafka.streams.kstream.*;

import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class RideRequestStreamProcessor {

    @Bean
    public KStream<String, RideRequest> processRideRequests(
            StreamsBuilder builder
    ) {

        JsonSerde<RideRequest> rideRequestSerde =
                new JsonSerde<>(RideRequest.class);

        JsonSerde<SurgeEvent> surgeEventSerde =
                new JsonSerde<>(SurgeEvent.class);
        /*

         *  STATE STORE

         */

        builder.addStateStore(

                Stores.keyValueStoreBuilder(

                        Stores.persistentKeyValueStore(

                                "zone-surge-store"

                        ),

                        Serdes.String(),

                        new JsonSerde<>(SurgeState.class)

                )

        );

        /*
         * STEP 1
         * Read ride requests from Kafka topic
         */
        KStream<String, RideRequest> rideRequestStream =

                builder.stream(

                        "ride-requests",

                        Consumed.with(
                                Serdes.String(),
                                rideRequestSerde
                        )
                );

        /*
         * STEP 2
         * Group + Window + Count
         */
        TimeWindowedKStream<String, RideRequest> groupedStream =

                rideRequestStream

                        .groupByKey()

                        .windowedBy(

                                TimeWindows.ofSizeWithNoGrace(
                                        Duration.ofSeconds(10)
                                )
                        );

        KTable<Windowed<String>, Long> rideCountTable =

                groupedStream.count(

                        Materialized

                                .<String, Long, WindowStore<Bytes, byte[]>>
                                        as("ride-count-store")

                                .withKeySerde(Serdes.String())

                                .withValueSerde(Serdes.Long())
                );

        /*
         * STEP 3
         * Convert counts into surge events
         */
        KStream<String, SurgeEvent> surgeStream =

                rideCountTable

                        .toStream()

                        .filter((windowedKey, count) -> count > 5)

                        .map((windowedKey, count) -> {

                            String zoneId = windowedKey.key();

                            double surgeMultiplier =
                                    calculateSurgeMultiplier(count);

                            SurgeEvent surgeEvent = new SurgeEvent(

                                    zoneId,

                                    count,

                                    surgeMultiplier,

                                    windowedKey.window()
                                            .startTime()
                                            .toString(),

                                    windowedKey.window()
                                            .endTime()
                                            .toString()
                            );

                            return KeyValue.pair(
                                    zoneId,
                                    surgeEvent
                            );
                        }).transform(

                                () -> new SurgeCooldownTransformer(),

                                "zone-surge-store"

                        );

        /*
         * STEP 4
         * Send surge events to Kafka topic
         */
        surgeStream.to(

                "surge-events",

                Produced.with(
                        Serdes.String(),
                        surgeEventSerde
                )
        );

        /*
         * STEP 5
         * Debug logging
         */
        surgeStream.foreach((key, value) -> {

            System.out.println(
                    "SURGE DETECTED → " + value
            );

        });

        return rideRequestStream;
    }

    /*
     * Business logic
     */
    private double calculateSurgeMultiplier(Long count) {

        if (count > 20) {
            return 3.0;
        }

        if (count > 10) {
            return 2.0;
        }

        return 1.5;
    }
}