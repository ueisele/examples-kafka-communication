package net.uweeisele.example.kafka.producer;

import net.uweeisele.examples.kafka.avro.v2.CountEvent;

import java.time.Instant;
import java.util.function.BiFunction;

import static java.lang.Long.parseLong;
import static java.util.UUID.randomUUID;

public class V2CountEventMapper implements BiFunction<String, String, CountEvent> {
    @Override
    public CountEvent apply(String key, String value) {
        return CountEvent.newBuilder()
                .setEventid(randomUUID().toString())
                .setCreated(Instant.now())
                .setSequenceid(key)
                .setValue(parseLong(value))
                .build();
    }
}
