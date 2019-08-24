package net.uweeisele.examples.kafka.serde.avro.deserializers.payload;

import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class Payload implements PayloadAware {

    private static final Key<Properties> KEY_CONFIGS = new Key<>("payload.configs");
    private static final Key<Properties> KEY_ATTRIBUTES = new Key<>("payload.attributes");

    private final Map<Key<?>, Object> content;

    public Payload() {
        this(new HashMap<>());
    }

    public Payload(PayloadAware payloadAware) {
        this(unwrap(requireNonNull(payloadAware)));
    }

    public Payload(Payload payload) {
        this(payload.content);
    }

    Payload(Map<Key<?>, Object> content) {
        this.content = content;
        if (attributes() == null) {
            setAttributes(new Properties());
        }
        if (configs() == null) {
            setConfigs(new Properties());
        }
    }

    private static Payload unwrap(PayloadAware payloadAware) {
        if (payloadAware instanceof Payload) {
            return (Payload) payloadAware;
        }
        throw new IllegalArgumentException(format("Given parameter is a %s, but expected a %s.", payloadAware.getClass().getName(), Payload.class.getName()));
    }

    protected <T> T get(Key<T> key) {
        return key.cast(content.get(key));
    }

    protected <T> Payload put(Key<T> key, T value) {
        content.put(key, value);
        return this;
    }

    protected <T> T computeIfAbsent(Key<T> key, Supplier<T> supplier) {
        return key.cast(content.computeIfAbsent(key, k -> supplier.get()));
    }

    public Map<Key<?>, ?> mapPayload() {
        return unmodifiableMap(content);
    }

    @Override
    public Map<String, String> printablePayload() {
        return content.entrySet().stream()
                .map(e -> Pair.of(e.getKey().name(), e.getKey().toString(e.getValue())))
                .collect(toMap(Pair::getKey, Pair::getValue));
    }

    @Override
    public String toString() {
        return printablePayload().toString();
    }

    @Override
    public Properties configs() {
        return get(KEY_CONFIGS);
    }

    @Override
    public Payload setConfigs(Properties configs) {
        return put(KEY_CONFIGS, requireNonNull(configs));
    }

    @Override
    public Properties attributes() {
        return get(KEY_ATTRIBUTES);
    }

    @Override
    public Payload setAttributes(Properties attributes) {
        return put(KEY_ATTRIBUTES, requireNonNull(attributes));
    }

    @Override
    public Payload withAttributes(Properties attributes) {
        computeIfAbsent(KEY_ATTRIBUTES, Properties::new).putAll(attributes);
        return this;
    }

    @Override
    public Payload withAttribute(String key, String value) {
        computeIfAbsent(KEY_ATTRIBUTES, Properties::new).setProperty(key, value);
        return this;
    }

    protected static class Key<T> {

        private final String name;

        private final Function<? super T, String> toString;

        public Key(String name) {
            this(name, String::valueOf);
        }

        public Key(String name, Function<? super T, String> toString) {
            this.name = requireNonNull(name);
            this.toString = requireNonNull(toString);
        }

        public String name() {
            return name;
        }

        public T cast(Object value) {
            return (T) value;
        }

        public String toString(Object value) {
            return value != null ? toString.apply(cast(value)) : null;
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
