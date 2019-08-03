package net.uweeisele.examples.kafka.transformer;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.Properties;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class KafkaStreamsBuilder implements Function<Properties, KafkaStreams>, Supplier<KafkaStreams> {

    private Function<Properties, Topology> topologyBuilder;

    private Supplier<Properties> propertiesSupplier = () -> new Properties();

    @Override
    public KafkaStreams get() {
        return build();
    }

    public KafkaStreams build() {
        return build(new Properties());
    }

    @Override
    public KafkaStreams apply(Properties properties) {
        return build(properties);
    }

    public KafkaStreams build(Properties properties) {
        Properties actualProperties = new Properties();
        actualProperties.putAll(propertiesSupplier.get());
        actualProperties.putAll(properties);
        return new KafkaStreams(topologyBuilder.apply(actualProperties), actualProperties);
    }

    public KafkaStreamsBuilder withTopology(Topology topology) {
        requireNonNull(topology);
        return withTopologyBuilder(p -> topology);
    }

    public KafkaStreamsBuilder withTopologyBuilder(Function<Properties, Topology> topologyBuilder) {
        this.topologyBuilder = requireNonNull(topologyBuilder);
        return this;
    }

    public KafkaStreamsBuilder withProperties(Properties properties) {
        requireNonNull(properties);
        return withPropertiesSupplier(() -> properties);
    }

    public KafkaStreamsBuilder withPropertiesSupplier(Supplier<Properties> propertiesSupplier) {
        this.propertiesSupplier = requireNonNull(propertiesSupplier);
        return this;
    }

}
