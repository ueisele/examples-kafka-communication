package net.uweeisele.examples.kafka.transformer;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.util.Properties;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class ConsumedTopic<K, V> extends Topic<K, V> {

    private final TimestampExtractor timestampExtractor;
    private final AutoOffsetReset resetPolicy;

    public ConsumedTopic(Topic<K, V> topic) {
        this(topic, null, null);
    }

    public ConsumedTopic(String name, Serde<K> keySerde, Serde<V> valueSerde) {
        this(name, keySerde, valueSerde, null, null);
    }

    public ConsumedTopic(Topic<K, V> topic, TimestampExtractor timestampExtractor, AutoOffsetReset resetPolicy) {
        this(topic.name(), topic.keySerde(), topic.valueSerde(), timestampExtractor, resetPolicy);
    }

    public ConsumedTopic(String name, Serde<K> keySerde, Serde<V> valueSerde, TimestampExtractor timestampExtractor, AutoOffsetReset resetPolicy) {
        super(name, keySerde, valueSerde);
        this.timestampExtractor = timestampExtractor;
        this.resetPolicy = resetPolicy;
    }

    public TimestampExtractor timestampExtractor() {
        return timestampExtractor;
    }

    public AutoOffsetReset resetPolicy() {
        return resetPolicy;
    }

    public Consumed<K, V> consumed() {
        return Consumed.with(keySerde, valueSerde, timestampExtractor, resetPolicy);
    }

    public static class Builder<K, V> implements Function<Properties, ConsumedTopic<K, V>> {

        private Function<Properties, ? extends Topic<K, V>> topicBuilder;
        private Function<Properties, ? extends TimestampExtractor> timestampExtractorBuilder = p -> null;
        private Function<Properties, AutoOffsetReset> resetPolicyBuilder = p -> null;

        public ConsumedTopic<K, V> build() {
            return build(new Properties());
        }

        @Override
        public ConsumedTopic<K, V> apply(Properties properties) {
            return build(properties);
        }

        public ConsumedTopic<K, V> build(Properties properties) {
            return new ConsumedTopic<>(topicBuilder.apply(properties),
                    timestampExtractorBuilder.apply(properties),
                    resetPolicyBuilder.apply(properties));
        }

        public Builder<K, V> withTopic(Topic<K, V> topic) {
            requireNonNull(topic);
            return withTopicBuilder(p -> topic);
        }

        public Builder<K, V> withTopicBuilder(Function<Properties, ? extends Topic<K, V>> topicBuilder) {
            this.topicBuilder = requireNonNull(topicBuilder);
            return this;
        }

        public Builder<K, V> withTimestampExtractor(TimestampExtractor timestampExtractor) {
            return withTimestampExtractorBuilder(p -> timestampExtractor);
        }

        public Builder<K, V> withTimestampExtractorBuilder(Function<Properties, ? extends TimestampExtractor> timestampExtractorBuilder) {
            this.timestampExtractorBuilder = requireNonNull(timestampExtractorBuilder);
            return this;
        }

        public Builder<K, V> withResetPolicy(AutoOffsetReset resetPolicy) {
            return withResetPolicyBuilder(p -> resetPolicy);
        }

        public Builder<K, V> withResetPolicyBuilder(Function<Properties, AutoOffsetReset> resetPolicyBuilder) {
            this.resetPolicyBuilder = requireNonNull(resetPolicyBuilder);
            return this;
        }

    }
}
