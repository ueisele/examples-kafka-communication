package net.uweeisele.examples.kafka.transformer;

import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.*;
import static java.util.Objects.requireNonNull;

public class AvroSerdeBuilder<T extends IndexedRecord> implements Function<Properties, Serde<T>>, Supplier<Serde<T>> {

    private final Supplier<Serde<T>> serdeSupplier;

    private final Properties internalProperties;

    private Supplier<Properties> propertiesSupplier = Properties::new;

    private Function<Properties, Boolean> isForKeyBuilder = p-> false;

    public AvroSerdeBuilder(Supplier<Serde<T>> serdeSupplier) {
            this(serdeSupplier, new Properties());
    }

    AvroSerdeBuilder(Supplier<Serde<T>> serdeSupplier, Properties internalProperties) {
        this.serdeSupplier = serdeSupplier;
        this.internalProperties = internalProperties;
    }

    public static <T extends IndexedRecord> AvroSerdeBuilder<T> avroSerdeBuilder() {
        return new AvroSerdeBuilder<>(IndexedAvroSerde::new);
    }

    public static AvroSerdeBuilder<GenericRecord> genericAvroSerdeBuilder() {
        return new AvroSerdeBuilder<>(GenericAvroSerde::new);
    }

    public static <T extends SpecificRecord> AvroSerdeBuilder<T> specificAvroSerdeBuilder() {
        return new AvroSerdeBuilder<>(SpecificAvroSerde::new);
    }

    @Override
    public Serde<T> get() {
        return build();
    }

    public Serde<T> build() {
        return build(new Properties());
    }


    @Override
    public Serde<T> apply(Properties properties) {
        return build(properties);
    }

    public Serde<T> build(Properties properties) {
        Properties actualConfig = new Properties();
        actualConfig.putAll(internalProperties);
        actualConfig.putAll(propertiesSupplier.get());
        actualConfig.putAll(properties);
        Serde<T> serde = serdeSupplier.get();
        serde.configure(toMap(actualConfig), isForKeyBuilder.apply(actualConfig));
        return serde;
    }

    public AvroSerdeBuilder<T> withProperties(Properties properties) {
        requireNonNull(properties);
        return withPropertiesSupplier(() -> properties);
    }

    public AvroSerdeBuilder<T> withPropertiesSupplier(Supplier<Properties> propertiesSupplier) {
        this.propertiesSupplier = requireNonNull(propertiesSupplier);
        return this;
    }

    public AvroSerdeBuilder<T> isForKey() {
        return withIsForKeyBuilder(p -> true);
    }

    public AvroSerdeBuilder<T> isForValue() {
        return withIsForKeyBuilder(p -> false);
    }

    public AvroSerdeBuilder<T> withIsForKeyBuilder(Function<Properties, Boolean> isForKeyBuilder) {
        this.isForKeyBuilder = requireNonNull(isForKeyBuilder);
        return this;
    }

    public AvroSerdeBuilder<T>  withSchemaRegistryUrl(String schemaRegistryUrl) {
        internalProperties.setProperty(SCHEMA_REGISTRY_URL_CONFIG, requireNonNull(schemaRegistryUrl));
        return this;
    }

    public AvroSerdeBuilder<T> withSubjectNameStrategy(Class<? extends SubjectNameStrategy> subjectNameStrategy) {
        internalProperties.setProperty(KEY_SUBJECT_NAME_STRATEGY, requireNonNull(subjectNameStrategy).getName());
        internalProperties.setProperty(VALUE_SUBJECT_NAME_STRATEGY, requireNonNull(subjectNameStrategy).getName());
        return this;
    }

    private static Map<String, Object> toMap(Properties properties) {
        return properties.entrySet().stream()
                .map(entry -> new SimpleEntry<>(String.valueOf(entry.getKey()), entry.getValue()))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

}
