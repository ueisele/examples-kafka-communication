package net.uweeisele.examples.kafka.transformer;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.StreamPartitioner;

import java.util.Properties;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class ProducedTopic<K, V> extends Topic<K, V> {

    private final StreamPartitioner<K, V> partitioner;

    public ProducedTopic(Topic<K,V> topic) {
        this(topic, null);
    }

    public ProducedTopic(String name, Serde<K> keySerde, Serde<V> valueSerde) {
        this(name, keySerde, valueSerde, null);
    }

    public ProducedTopic(Topic<K,V> topic, StreamPartitioner<K, V> partitioner) {
        this(topic.name(), topic.keySerde(), topic.valueSerde(), partitioner);
    }

    public ProducedTopic(String name, Serde<K> keySerde, Serde<V> valueSerde, StreamPartitioner<K, V> partitioner) {
        super(name, keySerde, valueSerde);
        this.partitioner = partitioner;
    }

    public StreamPartitioner<K, V> partitioner() {
        return partitioner;
    }

    public Produced<K, V> produced() {
        return Produced.with(keySerde, valueSerde, partitioner);
    }

    public static class Builder<K, V> implements Function<Properties, ProducedTopic<K, V>>, Supplier<ProducedTopic<K, V>> {

        private Function<Properties, ? extends Topic<K, V>> topicBuilder;
        private Function<Properties, ? extends StreamPartitioner<K, V>> partitionerBuilder = p -> null;

        @Override
        public ProducedTopic<K, V> get() {
            return build();
        }

        public ProducedTopic<K, V> build() {
            return build(new Properties());
        }

        @Override
        public ProducedTopic<K, V> apply(Properties properties) {
            return build(properties);
        }

        public ProducedTopic<K, V> build(Properties properties) {
            return new ProducedTopic<>(topicBuilder.apply(properties), partitionerBuilder.apply(properties));
        }

        public Builder<K, V> withTopic(Topic<K, V> topic) {
            requireNonNull(topic);
            return withTopicBuilder(p -> topic);
        }

        public Builder<K, V> withTopicBuilder(Function<Properties, ? extends Topic<K, V>> topicBuilder) {
            this.topicBuilder = requireNonNull(topicBuilder);
            return this;
        }

        public Builder<K, V> withPartitioner(StreamPartitioner<K, V> partitioner) {
            return withPartitionerBuilder(p -> partitioner);
        }

        public Builder<K, V> withPartitionerBuilder(Function<Properties, ? extends StreamPartitioner<K, V>> partitionerBuilder) {
            this.partitionerBuilder = requireNonNull(partitionerBuilder);
            return this;
        }

    }
}
