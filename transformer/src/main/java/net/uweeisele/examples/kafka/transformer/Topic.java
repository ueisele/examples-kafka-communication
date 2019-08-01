package net.uweeisele.examples.kafka.transformer;

import org.apache.kafka.common.serialization.Serde;

public class Topic<K, V> {

    protected final String name;
    protected final Serde<K> keySerde;
    protected final Serde<V> valueSerde;

    public Topic(String name, Serde<K> keySerde, Serde<V> valueSerde) {
        this.name = name;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
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

}
