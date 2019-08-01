package net.uweeisele.examples.kafka.transformer;

import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.KEY_SUBJECT_NAME_STRATEGY;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class AvroSerdeBuilder<T extends IndexedRecord> {

    private final Supplier<Serde<T>> serdeSupplier;

    private Map<String, ?> serdeConfig = new HashMap<>();

    private boolean isForKey = false;

    private String schemaRegistryUrl;

    private Class<? extends SubjectNameStrategy> subjectNameStrategy = TopicNameStrategy.class;

    public AvroSerdeBuilder(Supplier<Serde<T>> serdeSupplier) {
        this.serdeSupplier = serdeSupplier;
    }

    public static AvroSerdeBuilder<GenericRecord> genericAvroSerdeBuilder() {
        return new AvroSerdeBuilder<>(GenericAvroSerde::new);
    }

    public static <T extends SpecificRecord> AvroSerdeBuilder<T> specificAvroSerdeBuilder() {
        return new AvroSerdeBuilder<>(SpecificAvroSerde::new);
    }

    public Serde<T> build() {
        return build(Map.of());
    }

    public Serde<T> build(Properties serdeConfig) {
        return build(toMap(serdeConfig));
    }

    public Serde<T> build(Map<String, ?> serdeConfig) {
        Serde<T> serde = serdeSupplier.get();
        Map<String, Object> actualConfig = new HashMap<>();
        actualConfig.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        actualConfig.put(KEY_SUBJECT_NAME_STRATEGY, subjectNameStrategy.getName());
        actualConfig.putAll(this.serdeConfig);
        actualConfig.putAll(serdeConfig);
        serde.configure(actualConfig, isForKey);
        return serde;
    }

    public AvroSerdeBuilder<T> withSerdeConfig(Properties serdeConfig) {
        return withSerdeConfig(toMap(serdeConfig));
    }

    public AvroSerdeBuilder<T> withSerdeConfig(Map<String, ?> serdeConfig) {
        this.serdeConfig = serdeConfig;
        return this;
    }

    public AvroSerdeBuilder<T> isForKey() {
        isForKey = true;
        return this;
    }

    public AvroSerdeBuilder<T> isForValue() {
        isForKey = false;
        return this;
    }

    public AvroSerdeBuilder<T>  withSchemaRegistryUrl(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        return this;
    }

    public AvroSerdeBuilder<T> withSubjectNameStrategy(Class<? extends SubjectNameStrategy> subjectNameStrategy) {
        this.subjectNameStrategy = subjectNameStrategy;
        return this;
    }

    private static Map<String, Object> toMap(Properties properties) {
        return properties.entrySet().stream()
                .map(entry -> new SimpleEntry<>(String.valueOf(entry.getKey()), entry.getValue()))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }
}
