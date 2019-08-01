package net.uweeisele.examples.kafka.transformer;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

import static java.util.Objects.requireNonNull;

public class TransformerStreamsBuilder<KS, VS, KD, VD> {

    private Topology topology;

    private Properties properties;

    public KafkaStreams build() {
        return build(new Properties());
    }

    public KafkaStreams build(Properties properties) {
        Properties actualProperties = new Properties();
        actualProperties.putAll(this.properties);
        actualProperties.putAll(properties);
        return new KafkaStreams(topology, actualProperties);
    }

    public TransformerStreamsBuilder<KS, VS, KD, VD> withTopology(Topology topology) {
        this.topology = requireNonNull(topology);
        return this;
    }

    public TransformerStreamsBuilder<KS, VS, KD, VD> withProperties(Properties properties) {
        this.properties = requireNonNull(properties);
        return this;
    }
}
