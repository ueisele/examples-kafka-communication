package net.uweeisele.examples.kafka.avro.serializers.payload;

import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class Payload {

    private final Map<Key<?>, Object> content;

    public Payload() {
        this(new HashMap<>());
    }

    public Payload(Payload payload) {
        this(payload.content);
    }

    Payload(Map<Key<?>, Object> content) {
        this.content = content;
    }

    public <T> T get(Key<T> key) {
        return key.cast(content.get(key));
    }

    public <T> Payload put(Key<T> key, T value) {
        content.put(key, value);
        return this;
    }

    public Map<Key<?>, ?> content() {
        return unmodifiableMap(content);
    }

    public Map<String, String> printableContent() {
        return content.entrySet().stream()
                .map(e -> Pair.of(e.getKey().toString(), e.getValue() != null ? e.getValue().toString() : null))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    public static class Key<T> {

        private final String name;

        public Key(String name) {
            this.name = requireNonNull(name);
        }

        public String name() {
            return name;
        }

        public T cast(Object value) {
            return (T) value;
        }

        @Override
        public String toString() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key<?> key = (Key<?>) o;
            return name.equals(key.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }
    }
}
