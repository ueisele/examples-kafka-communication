package net.uweeisele.examples.kafka.transformer;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class ProducedTopic<K, V> extends Topic<K, V> {

    private final StreamPartitioner<K, V> partitioner;

    public ProducedTopic(String name, Serde<K> keySerde, Serde<V> valueSerde) {
        this(name, keySerde, valueSerde, null);
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
}
