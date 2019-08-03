package net.uweeisele.examples.kafka.transformer;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.Properties;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class TransformerStreamsBuilder {

    private Function<Properties, Topology> topologyBuilder;

    private Properties properties;

    public KafkaStreams build() {
        return build(new Properties());
    }

    public KafkaStreams build(Properties properties) {
        Properties actualProperties = new Properties();
        actualProperties.putAll(this.properties);
        actualProperties.putAll(properties);
        return new KafkaStreams(topologyBuilder.apply(actualProperties), actualProperties);
    }

    public TransformerStreamsBuilder withTopology(Topology topology) {
        requireNonNull(topology);
        return withTopologyBuilder(p -> topology);
    }

    public TransformerStreamsBuilder withTopologyBuilder(Function<Properties, Topology> topologyBuilder) {
        this.topologyBuilder = requireNonNull(topologyBuilder);
        return this;
    }

    public TransformerStreamsBuilder withProperties(Properties properties) {
        this.properties = requireNonNull(properties);
        return this;
    }
}
