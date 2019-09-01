package net.uweeisele.examples.kafka.serde.avro.protocol;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class ContextSupplierBuilder implements ContextSupplier {

    private final Map<String, String> map = new HashMap<>();

    @Override
    public Map<String, String> context() {
        return map;
    }

    public ContextSupplierBuilder with(String key, Object value) {
        return with(key, value != null ? String.valueOf(value) : null);
    }

    public ContextSupplierBuilder with(String key, String value) {
        map.put(key, value);
        return this;
    }

    public ContextSupplierBuilder withAll(Map<String, String> map) {
        this.map.putAll(map);
        return this;
    }

    public ContextSupplierBuilder withAllNormalized(Map<?, ?> map) {
        return withAllNormalized(map, String::valueOf, String::valueOf);
    }

    public <K, V> ContextSupplierBuilder withAllNormalized(Map<K, V> map, Function<K, String> keyNormalizer, Function<V, String> valueNormalizer) {
        map.entrySet().stream()
                .filter(entry -> entry.getKey() != null && entry.getValue() != null)
                .forEach(entry -> this.map.put(keyNormalizer.apply(entry.getKey()), valueNormalizer.apply(entry.getValue())));
        return this;
    }

    public static ContextSupplierBuilder contextSupplier() {
        return new ContextSupplierBuilder();
    }

}
