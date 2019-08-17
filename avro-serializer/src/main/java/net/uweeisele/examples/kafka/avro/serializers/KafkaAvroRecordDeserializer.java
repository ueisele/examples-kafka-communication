package net.uweeisele.examples.kafka.avro.serializers;

import net.uweeisele.examples.kafka.avro.serializers.builder.SchemaRegistryClientBuilder;
import net.uweeisele.examples.kafka.avro.serializers.enricher.ReaderSchemaEnricher;
import net.uweeisele.examples.kafka.avro.serializers.extractor.AvroRecordPayloadExtractor;
import net.uweeisele.examples.kafka.avro.serializers.extractor.DatumReaderPayloadExtractor;
import net.uweeisele.examples.kafka.avro.serializers.extractor.SchemaRegistryWriterSchemaPayloadExtractor;
import net.uweeisele.examples.kafka.avro.serializers.extractor.WriterSchemaIdPayloadExtractor;
import net.uweeisele.examples.kafka.avro.serializers.extractor.deserializer.DatumReaderBuilder;
import net.uweeisele.examples.kafka.avro.serializers.function.ConfigurableBiFunction;
import net.uweeisele.examples.kafka.avro.serializers.function.ConfigurableFunction;
import net.uweeisele.examples.kafka.avro.serializers.payload.AvroBytesPayload;
import net.uweeisele.examples.kafka.avro.serializers.payload.AvroBytesWriterReaderSchemaPayload;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

public class KafkaAvroRecordDeserializer<T extends IndexedRecord> implements Deserializer<T> {

    private final ConfigurableBiFunction<byte[], Schema, T> deserializer;

    public KafkaAvroRecordDeserializer(DatumReaderBuilder<T> datumReaderBuilder, SchemaRegistryClientBuilder schemaRegistryClientBuilder, Properties properties) {
        this(datumReaderBuilder, schemaRegistryClientBuilder);
        configure(properties);
    }

    public KafkaAvroRecordDeserializer(DatumReaderBuilder<T> datumReaderBuilder, SchemaRegistryClientBuilder schemaRegistryClientBuilder) {
        this(buildDeserializerFunction(datumReaderBuilder, schemaRegistryClientBuilder));
    }

    public KafkaAvroRecordDeserializer(ConfigurableBiFunction<byte[], Schema, T> deserializer, Properties properties) {
        this(deserializer);
        configure(properties);
    }

    public KafkaAvroRecordDeserializer(ConfigurableBiFunction<byte[], Schema, T> deserializer) {
        this.deserializer = requireNonNull(deserializer);
    }

    private static <T extends IndexedRecord> ConfigurableBiFunction<byte[], Schema, T> buildDeserializerFunction(DatumReaderBuilder<T> datumReaderBuilder, SchemaRegistryClientBuilder schemaRegistryClientBuilder) {
        return ConfigurableFunction.<byte[]>identity()
                .andThen(AvroBytesPayload::new)
                .andThen(new WriterSchemaIdPayloadExtractor<>())
                .andThen(new SchemaRegistryWriterSchemaPayloadExtractor<>(schemaRegistryClientBuilder))
                .andThen(new ReaderSchemaEnricher<>(AvroBytesWriterReaderSchemaPayload::new))
                .andThen(new DatumReaderPayloadExtractor<>(datumReaderBuilder))
                .andThen(new AvroRecordPayloadExtractor<>());
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        configure(configs);
    }

    public void configure(Map<String, ?> configs) {
        Properties properties = new Properties();
        properties.putAll(configs);
        configure(properties);
    }

    public void configure(Properties properties) {
        deserializer.configure(properties);
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        return deserialize(topic, data, null);
    }

    public T deserialize(String topic, byte[] data, Schema readerSchema) {
        return deserializer.apply(data, readerSchema);
    }
}
