package net.uweeisele.example.kafka.consumer;

import net.uweeisele.examples.kafka.avro.v1.CountEvent;

import java.util.function.BiFunction;

public class V1CountEventMapper implements BiFunction<String, CountEvent, String> {
    @Override
    public String apply(String key, CountEvent value) {
        return value != null ? String.valueOf(value.getNumber()) : null;
    }
}
