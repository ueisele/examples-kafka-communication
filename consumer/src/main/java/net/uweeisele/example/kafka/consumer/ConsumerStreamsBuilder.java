package net.uweeisele.example.kafka.consumer;

import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import net.uweeisele.examples.kafka.transformer.*;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.Properties;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.KEY_SUBJECT_NAME_STRATEGY;
import static java.util.Objects.requireNonNull;

public class ConsumerStreamsBuilder<T extends IndexedRecord> implements Function<Properties, KafkaStreams>, Supplier<KafkaStreams> {

    private static final String KEY_TOPIC_SOURCE_NAME = "topic.source.name";
    private static final String KEY_TOPIC_DESTINATION_NAME = "topic.destination.name";

    private final AvroSerdeBuilder<T> avroSerdeBuilder;

    private final KafkaStreamsBuilder kafkaStreamsBuilder;
    private final TransformerTopologyBuilder<String, T, String, String> topologyBuilder;
    private final KeyValueMapperList<String, T, String, String> keyValueMappers;
    private final Properties internalProperties;

    private Supplier<Properties> propertiesSupplier = Properties::new;

    public ConsumerStreamsBuilder(AvroSerdeBuilder<T> avroSerdeBuilder) {
        this(avroSerdeBuilder, new KafkaStreamsBuilder(), new TransformerTopologyBuilder<>(), new KeyValueMapperList<>(), new Properties());
    }

    ConsumerStreamsBuilder(AvroSerdeBuilder<T> avroSerdeBuilder,
                           KafkaStreamsBuilder kafkaStreamsBuilder,
                           TransformerTopologyBuilder<String, T, String, String> topologyBuilder,
                           KeyValueMapperList<String, T, String, String> keyValueMappers,
                           Properties internalProperties) {
        this.avroSerdeBuilder = avroSerdeBuilder;
        this.kafkaStreamsBuilder = kafkaStreamsBuilder;
        this.topologyBuilder = topologyBuilder;
        this.keyValueMappers = keyValueMappers;
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
                    .withSourceTopicBuilder(new ConsumedTopic.Builder<String, T>()
                        .withTopicBuilder(new Topic.Builder<String, T>()
                            .withNameBuilder(new RequiredValueBuilder(KEY_TOPIC_SOURCE_NAME))
                            .withKeySerde(Serdes.String())
                            .withValueSerdeBuilder(avroSerdeBuilder
                                    .isForValue()
                                    .withSubjectNameStrategy(TopicRecordNameStrategy.class))
                        ))
                    .withKeyValueMapper(keyValueMappers)
                    .withDestinationTopicBuilder(new ProducedTopic.Builder<String, String>()
                         .withTopicBuilder(new Topic.Builder<String, String>()
                            .withNameBuilder(new RequiredValueBuilder(KEY_TOPIC_DESTINATION_NAME))
                            .withKeySerde(Serdes.String())
                            .withValueSerde(Serdes.String()))
                        ))
                .build(actualProperties);
    }

    public ConsumerStreamsBuilder<T> withProperties(Properties properties) {
        requireNonNull(properties);
        return withPropertiesSupplier(() -> properties);
    }

    public ConsumerStreamsBuilder<T> withPropertiesSupplier(Supplier<Properties> propertiesSupplier) {
        this.propertiesSupplier = requireNonNull(propertiesSupplier);
        return this;
    }

    public ConsumerStreamsBuilder<T> withSourceTopicName(String name) {
        internalProperties.setProperty(KEY_TOPIC_SOURCE_NAME, requireNonNull(name));
        return this;
    }

    public ConsumerStreamsBuilder<T> withDestinationTopicName(String name) {
        internalProperties.setProperty(KEY_TOPIC_DESTINATION_NAME, requireNonNull(name));
        return this;
    }

    public ConsumerStreamsBuilder<T> withSubjectNameStrategy(Class<? extends SubjectNameStrategy> subjectNameStrategy) {
        internalProperties.setProperty(KEY_SUBJECT_NAME_STRATEGY, requireNonNull(subjectNameStrategy).getName());
        return this;
    }

    public ConsumerStreamsBuilder<T> withKeyValueMapper(KeyValueMapper<String, T, KeyValue<? extends String, ? extends String>> keyValueMapper) {
        keyValueMappers.with(keyValueMapper);
        return this;
    }

}
