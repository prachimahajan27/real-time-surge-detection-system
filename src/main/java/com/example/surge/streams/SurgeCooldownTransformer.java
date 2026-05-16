package com.example.surge.streams;



import com.example.surge.model.SurgeEvent;

import org.apache.kafka.streams.KeyValue;

import org.apache.kafka.streams.kstream.Transformer;

import org.apache.kafka.streams.processor.ProcessorContext;

import org.apache.kafka.streams.state.KeyValueStore;

public class SurgeCooldownTransformer implements
        Transformer<String, SurgeEvent, KeyValue<String, SurgeEvent>> {

    private KeyValueStore<String, Long> stateStore;

    private static final long COOLDOWN_MS = 60_000;

    @Override
    public void init(ProcessorContext context) {

        stateStore = context.getStateStore(
                "zone-surge-store"
        );
    }

    @Override
    public KeyValue<String, SurgeEvent> transform(
            String key,
            SurgeEvent value
    ) {

        System.out.println(

                "TRANSFORM CALLED FOR ZONE → " + key

        );
        Long lastSurgeTime = stateStore.get(key);

        long currentTime = System.currentTimeMillis();

        /*
         * First surge ever
         */
        if (lastSurgeTime == null) {
            System.out.println(

                    "FIRST SURGE ALLOWED → " + key

            );

            stateStore.put(key, currentTime);

            return KeyValue.pair(key, value);
        }

        /*
         * Cooldown expired
         */
        if (currentTime - lastSurgeTime > COOLDOWN_MS) {

            System.out.println(

                    "COOLDOWN EXPIRED → " + key

            );
            stateStore.put(key, currentTime);

            return KeyValue.pair(key, value);
        }

        /*
         * Suppress duplicate surge
         */
        return null;
    }

    @Override
    public void close() {
    }
}