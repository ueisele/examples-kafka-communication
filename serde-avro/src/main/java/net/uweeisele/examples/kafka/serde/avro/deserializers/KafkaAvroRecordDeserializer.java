package net.uweeisele.examples.kafka.serde.avro.deserializers;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import net.uweeisele.examples.kafka.serde.avro.builder.SchemaRegistryClientBuilder;
import net.uweeisele.examples.kafka.serde.avro.deserializers.function.record.AvroRecordDeserializer;
import net.uweeisele.examples.kafka.serde.avro.deserializers.function.schema.DataSchemaIdDeserializer;
import net.uweeisele.examples.kafka.serde.avro.deserializers.function.schema.SchemaRegistryWriterDeserializer;
import net.uweeisele.examples.kafka.serde.avro.deserializers.payload.AvroDeserializablePayload;
import net.uweeisele.examples.kafka.serde.avro.deserializers.function.record.DatumReaderBuilder;
import org.apache.avro.generic.IndexedRecord;

import java.util.Properties;
import java.util.function.Function;

public class KafkaAvroRecordDeserializer<T extends IndexedRecord> extends BaseKafkaAvroDeserializer<AvroDeserializablePayload, T> {

    public KafkaAvroRecordDeserializer(DatumReaderBuilder<AvroDeserializablePayload, T> datumReaderBuilder) {
        this(new DataSchemaIdDeserializer<>(), new SchemaRegistryClientBuilder(), datumReaderBuilder);
    }

    public KafkaAvroRecordDeserializer(Function<? super AvroDeserializablePayload, Integer> schemaIdDeserializer, Function<Properties, SchemaRegistryClient> schemaRegistryClientBuilder, DatumReaderBuilder<AvroDeserializablePayload, T> datumReaderBuilder) {
        super(AvroDeserializablePayload::new, new SchemaRegistryWriterDeserializer<>(schemaIdDeserializer, schemaRegistryClientBuilder, p -> p), new AvroRecordDeserializer<>(datumReaderBuilder));
    }

}
