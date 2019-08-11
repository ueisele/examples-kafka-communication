package net.uweeisele.example.kafka.producer;

import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import net.uweeisele.examples.kafka.transformer.*;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;

import java.util.Properties;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.KEY_SUBJECT_NAME_STRATEGY;
import static java.lang.String.join;
import static java.util.Objects.requireNonNull;
import static net.uweeisele.examples.kafka.transformer.AvroSerdeBuilder.avroSerdeBuilder;
import static net.uweeisele.examples.kafka.transformer.Transformation.valueTransformation;

public class ProducerStreamsBuilder implements Function<Properties, KafkaStreams>, Supplier<KafkaStreams> {

    private static final String KEY_TOPIC_SOURCE_NAME = "topic.source.name";
    private static final String KEY_TOPIC_DESTINATION_NAME = "topic.destination.name";
    private static final String KEY_MAPPERS = "mappers";
    private static final String MAPPERS_VALUE_DELIMITER = ",";

    private final KafkaStreamsBuilder kafkaStreamsBuilder;
    private final TransformerTopologyBuilder<String, String, String, IndexedRecord> topologyBuilder;
    private final PropertiesKeyValueMapperBuilder<String, String, String, IndexedRecord> eventMapperBuilder;
    private final Properties internalProperties;

    private Supplier<Properties> propertiesSupplier = Properties::new;

    public ProducerStreamsBuilder() {
        this(new KafkaStreamsBuilder(), new TransformerTopologyBuilder<>(), new PropertiesKeyValueMapperBuilder<>(new ListValueBuilder(new RequiredValueBuilder(KEY_MAPPERS), MAPPERS_VALUE_DELIMITER)), new Properties());
    }

    ProducerStreamsBuilder(KafkaStreamsBuilder kafkaStreamsBuilder,
                           TransformerTopologyBuilder<String, String, String, IndexedRecord> topologyBuilder,
                           PropertiesKeyValueMapperBuilder<String, String, String, IndexedRecord> eventMapperBuilder,
                           Properties internalProperties) {
        this.kafkaStreamsBuilder = kafkaStreamsBuilder;
        this.topologyBuilder = topologyBuilder;
        this.eventMapperBuilder = eventMapperBuilder;
        this.internalProperties = internalProperties;
    }

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
        actualProperties.putAll(internalProperties);
        actualProperties.putAll(propertiesSupplier.get());
        actualProperties.putAll(properties);
        return kafkaStreamsBuilder
                .withTopologyBuilder(topologyBuilder
                    .withSourceTopicBuilder(new ConsumedTopic.Builder<String, String>()
                        .withTopicBuilder(new Topic.Builder<String, String>()
                            .withNameBuilder(new RequiredValueBuilder(KEY_TOPIC_SOURCE_NAME))
                            .withKeySerde(Serdes.String())
                            .withValueSerde(Serdes.String())
                        ))
                    .withKeyValueMapperBuilder(eventMapperBuilder)
                    .withDestinationTopicBuilder(new ProducedTopic.Builder<String, IndexedRecord>()
                            .withTopicBuilder(new Topic.Builder<String, IndexedRecord>()
                            .withNameBuilder(new RequiredValueBuilder(KEY_TOPIC_DESTINATION_NAME))
                            .withKeySerde(Serdes.String())
                            .withValueSerdeBuilder(avroSerdeBuilder()
                                .isForValue()
                                .withSubjectNameStrategy(TopicRecordNameStrategy.class)))
                        ))
                .build(actualProperties);
    }

    public ProducerStreamsBuilder withProperties(Properties properties) {
        requireNonNull(properties);
        return withPropertiesSupplier(() -> properties);
    }

    public ProducerStreamsBuilder withPropertiesSupplier(Supplier<Properties> propertiesSupplier) {
        this.propertiesSupplier = requireNonNull(propertiesSupplier);
        return this;
    }

    public ProducerStreamsBuilder withRegisteredMapper(String key, Supplier<? extends BiFunction<String, String, ? extends IndexedRecord>> mapperSupplier) {
        eventMapperBuilder.withRegisteredKeyValueMapper(requireNonNull(key), valueTransformation(requireNonNull(mapperSupplier)));
        return this;
    }

    public ProducerStreamsBuilder withDefaultSourceTopicName(String name) {
        internalProperties.setProperty(KEY_TOPIC_SOURCE_NAME, requireNonNull(name));
        return this;
    }

    public ProducerStreamsBuilder withDefaultDestinationTopicName(String name) {
        internalProperties.setProperty(KEY_TOPIC_DESTINATION_NAME, requireNonNull(name));
        return this;
    }

    public ProducerStreamsBuilder withDefaultSubjectNameStrategy(Class<? extends SubjectNameStrategy> subjectNameStrategy) {
        internalProperties.setProperty(KEY_SUBJECT_NAME_STRATEGY, requireNonNull(subjectNameStrategy).getName());
        return this;
    }

    public ProducerStreamsBuilder withDefaultMappers(String... mappers) {
        internalProperties.setProperty(KEY_MAPPERS, join(MAPPERS_VALUE_DELIMITER, requireNonNull(mappers)));
        return this;
    }

}
