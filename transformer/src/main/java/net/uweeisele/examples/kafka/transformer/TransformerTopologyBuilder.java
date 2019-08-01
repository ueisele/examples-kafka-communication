package net.uweeisele.examples.kafka.transformer;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import static java.util.Objects.requireNonNull;
import static net.uweeisele.examples.kafka.transformer.KeyValueMapperList.none;

public class TransformerTopologyBuilder<KS, VS, KD, VD> {

    private final ConsumedTopic<KS, VS> sourceTopic;

    private final ProducedTopic<KD, VD> destinationTopic;

    private KeyValueMapper<? super KS, ? super VS, ? extends Iterable<? extends KeyValue<? extends KD, ? extends VD>>> keyValueMapper;

    public TransformerTopologyBuilder(ConsumedTopic<KS, VS> sourceTopic,
                                      ProducedTopic<KD, VD> destinationTopic) {
        this(sourceTopic, destinationTopic, none());
    }

    public TransformerTopologyBuilder(ConsumedTopic<KS, VS> sourceTopic,
                                      ProducedTopic<KD, VD> destinationTopic,
                                      KeyValueMapper<? super KS, ? super VS, ? extends Iterable<? extends KeyValue<? extends KD, ? extends VD>>> keyValueMapper) {
        this.sourceTopic = sourceTopic;
        this.destinationTopic = destinationTopic;
        this.keyValueMapper = keyValueMapper;
    }

    public Topology build() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(sourceTopic.name(), sourceTopic.consumed())
                .flatMap(keyValueMapper)
                .to(sourceTopic.name(), destinationTopic.produced());
        return builder.build();
    }

    public TransformerTopologyBuilder<KS, VS, KD, VD> withKeyValueMapper(KeyValueMapper<? super KS, ? super VS, ? extends Iterable<? extends KeyValue<? extends KD, ? extends VD>>> keyValueMapper) {
        this.keyValueMapper = requireNonNull(keyValueMapper);
        return this;
    }

}
