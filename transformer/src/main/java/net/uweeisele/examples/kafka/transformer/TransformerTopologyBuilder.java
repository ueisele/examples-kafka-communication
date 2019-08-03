package net.uweeisele.examples.kafka.transformer;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.Properties;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static net.uweeisele.examples.kafka.transformer.KeyValueMapperList.none;

public class TransformerTopologyBuilder<KS, VS, KD, VD> implements Function<Properties, Topology> {

    private final Function<Properties, ConsumedTopic<KS, VS>> sourceTopicBuilder;

    private final Function<Properties, ProducedTopic<KD, VD>> destinationTopicBuilder;

    private Function<Properties, ? extends KeyValueMapper<? super KS, ? super VS, ? extends Iterable<? extends KeyValue<? extends KD, ? extends VD>>>> keyValueMapperBuilder;

    public TransformerTopologyBuilder(ConsumedTopic<KS, VS> sourceTopic,
                                      ProducedTopic<KD, VD> destinationTopic) {
        this(p -> sourceTopic, p-> destinationTopic);
    }

    public TransformerTopologyBuilder(ConsumedTopic<KS, VS> sourceTopic,
                                      ProducedTopic<KD, VD> destinationTopic,
                                      KeyValueMapper<? super KS, ? super VS, ? extends Iterable<? extends KeyValue<? extends KD, ? extends VD>>> keyValueMapper) {
        this(p -> sourceTopic, p -> destinationTopic, p -> keyValueMapper);
    }

    public TransformerTopologyBuilder(Function<Properties, ConsumedTopic<KS, VS>> sourceTopicBuilder,
                                      Function<Properties, ProducedTopic<KD, VD>> destinationTopicBuilder) {
        this(sourceTopicBuilder, destinationTopicBuilder, p -> none());
    }

    public TransformerTopologyBuilder(Function<Properties, ConsumedTopic<KS, VS>> sourceTopicBuilder,
                                      Function<Properties, ProducedTopic<KD, VD>> destinationTopicBuilder,
                                      Function<Properties, ? extends KeyValueMapper<? super KS, ? super VS, ? extends Iterable<? extends KeyValue<? extends KD, ? extends VD>>>> keyValueMapperBuilder) {
        this.sourceTopicBuilder = sourceTopicBuilder;
        this.destinationTopicBuilder = destinationTopicBuilder;
        this.keyValueMapperBuilder = keyValueMapperBuilder;
    }

    public Topology build() {
        return build(new Properties());
    }

    @Override
    public Topology apply(Properties properties) {
        return build(properties);
    }

    public Topology build(Properties properties) {
        return build(sourceTopicBuilder.apply(properties),
                destinationTopicBuilder.apply(properties),
                keyValueMapperBuilder.apply(properties));
    }

    public Topology build(ConsumedTopic<KS, VS> sourceTopic, ProducedTopic<KD, VD> destinationTopic, KeyValueMapper<? super KS, ? super VS, ? extends Iterable<? extends KeyValue<? extends KD, ? extends VD>>> keyValueMapper) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(sourceTopic.name(), sourceTopic.consumed())
                .flatMap(keyValueMapper)
                .to(destinationTopic.name(), destinationTopic.produced());
        return builder.build();
    }

    public TransformerTopologyBuilder<KS, VS, KD, VD> withKeyValueMapper(KeyValueMapper<? super KS, ? super VS, ? extends Iterable<? extends KeyValue<? extends KD, ? extends VD>>> keyValueMapper) {
        requireNonNull(keyValueMapper);
        return withKeyValueMapperBuilder(p -> keyValueMapper);
    }

    public TransformerTopologyBuilder<KS, VS, KD, VD> withKeyValueMapperBuilder(Function<Properties, ? extends KeyValueMapper<? super KS, ? super VS, ? extends Iterable<? extends KeyValue<? extends KD, ? extends VD>>>> keyValueMapperBuilder) {
        this.keyValueMapperBuilder = requireNonNull(keyValueMapperBuilder);
        return this;
    }

}
