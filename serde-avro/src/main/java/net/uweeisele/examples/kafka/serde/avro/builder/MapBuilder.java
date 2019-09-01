package net.uweeisele.examples.kafka.serde.avro.builder;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class MapBuilder<K, V> implements Supplier<Map<K, V>> {

    private final Map<K, V> map = new HashMap<>();

    @Override
    public Map<K, V> get() {
        return build();
    }

    public Map<K, V> build() {
        return map;
    }

    public MapBuilder<K, V> withEntry(K key, V value) {
        map.put(key, value);
        return this;
    }

    public MapBuilder<K, V> withAll(Map<K, V> map) {
        this.map.putAll(map);
        return this;
    }

    public <U, S> MapBuilder<K, V> withAllNormalized(Map<U, S> map, Function<U, K> keyNormalizer, Function<S, V> valueNormalizer) {
        map.entrySet().stream()
                .filter(entry -> entry.getKey() != null && entry.getValue() != null)
                .forEach(entry -> this.map.put(keyNormalizer.apply(entry.getKey()), valueNormalizer.apply(entry.getValue())));
        return this;
    }

    public static <K, V> Map<K, V> of(K key, V value) {
        return new MapBuilder<K, V>()
                .withEntry(key, value)
                .build();
    }
}
