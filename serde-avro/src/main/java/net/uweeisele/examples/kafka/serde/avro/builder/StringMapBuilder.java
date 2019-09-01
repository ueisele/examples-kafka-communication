package net.uweeisele.examples.kafka.serde.avro.builder;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class StringMapBuilder implements Supplier<Map<String, String>> {

    private final Map<String, String> map = new HashMap<>();

    @Override
    public Map<String, String> get() {
        return build();
    }

    public Map<String, String> build() {
        return map;
    }

    public StringMapBuilder withEntry(String key, String value) {
        map.put(key, value);
        return this;
    }

    public StringMapBuilder withAll(Map<String, String> map) {
        this.map.putAll(map);
        return this;
    }

    public StringMapBuilder withAllNormalized(Map<?, ?> map) {
        return withAllNormalized(map, String::valueOf, String::valueOf);
    }

    public <K, V> StringMapBuilder withAllNormalized(Map<K, V> map, Function<K, String> keyNormalizer, Function<V, String> valueNormalizer) {
        map.entrySet().stream()
                .filter(entry -> entry.getKey() != null && entry.getValue() != null)
                .forEach(entry -> this.map.put(keyNormalizer.apply(entry.getKey()), valueNormalizer.apply(entry.getValue())));
        return this;
    }

    public static Map<String, String> of(String key, String value) {
        return new StringMapBuilder()
                .withEntry(key, value)
                .build();
    }
}
