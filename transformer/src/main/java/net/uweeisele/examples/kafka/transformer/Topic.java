package net.uweeisele.examples.kafka.transformer;

import org.apache.kafka.common.serialization.Serde;

import java.util.Properties;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class Topic<K, V> {

    protected final String name;
    protected final Serde<K> keySerde;
    protected final Serde<V> valueSerde;

    public Topic(Topic<K, V> topic) {
        this(topic.name, topic.keySerde, topic.valueSerde);
    }

    public Topic(String name, Serde<K> keySerde, Serde<V> valueSerde) {
        this.name = requireNonNull(name);
        this.keySerde = requireNonNull(keySerde);
        this.valueSerde = requireNonNull(valueSerde);
    }

    public String name() {
        return name;
    }

    public Serde<K> keySerde() {
        return keySerde;
    }

    public Serde<V> valueSerde() {
        return valueSerde;
    }

    public static class Builder<K, V> implements Function<Properties, Topic<K, V>>, Supplier<Topic<K, V>> {

        private Function<Properties, String> nameBuilder;
        private Function<Properties, ? extends Serde<K>> keySerdeBuilder;
        private Function<Properties, ? extends Serde<V>> valueSerdeBuilder;

        @Override
        public Topic<K, V> get() {
            return build();
        }

        public Topic<K, V> build() {
            return build(new Properties());
        }

        @Override
        public Topic<K, V> apply(Properties properties) {
            return build(properties);
        }

        public Topic<K, V> build(Properties properties) {
            return new Topic<>(nameBuilder.apply(properties),
                    keySerdeBuilder.apply(properties),
                    valueSerdeBuilder.apply(properties));
        }

        public Builder<K, V> withName(String name) {
            requireNonNull(name);
            return withNameBuilder(p -> name);
        }

        public Builder<K, V> withNameBuilder(Function<Properties, String> nameBuilder) {
            this.nameBuilder = requireNonNull(nameBuilder);
            return this;
        }

        public Builder<K, V> withKeySerde(Serde<K> keySerde) {
            requireNonNull(keySerde);
            return withKeySerdeBuilder(p -> keySerde);
        }

        public Builder<K, V> withKeySerdeBuilder(Function<Properties, ? extends Serde<K>> keySerdeBuilder) {
            this.keySerdeBuilder = requireNonNull(keySerdeBuilder);
            return this;
        }

        public Builder<K, V> withValueSerde(Serde<V> valueSerde) {
            requireNonNull(valueSerde);
            return withValueSerdeBuilder(p -> valueSerde);
        }

        public Builder<K, V> withValueSerdeBuilder(Function<Properties, ? extends Serde<V>> valueSerdeBuilder) {
            this.valueSerdeBuilder = requireNonNull(valueSerdeBuilder);
            return this;
        }

    }
}
