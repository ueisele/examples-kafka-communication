package net.uweeisele.examples.kafka.transformer;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class ConsumedTopic<K, V> extends Topic<K, V> {

    private final TimestampExtractor timestampExtractor;
    private final AutoOffsetReset resetPolicy;

    public ConsumedTopic(String name, Serde<K> keySerde, Serde<V> valueSerde) {
        this(name, keySerde, valueSerde, null, null);
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
}
