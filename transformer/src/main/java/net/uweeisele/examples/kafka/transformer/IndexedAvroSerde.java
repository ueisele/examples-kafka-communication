package net.uweeisele.examples.kafka.transformer;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class IndexedAvroSerde<T extends IndexedRecord> implements Serde<T> {

    private final Serde<T> inner;

    public IndexedAvroSerde() {
        this.inner = Serdes.serdeFrom(new IndexedAvroSerializer<>(), new IndexedAvroDeserializer<>());
    }

    public IndexedAvroSerde(SchemaRegistryClient client) {
        if (client == null) {
            throw new IllegalArgumentException("schema registry client must not be null");
        } else {
            this.inner = Serdes.serdeFrom(new IndexedAvroSerializer<>(client), new IndexedAvroDeserializer<>(client));
        }
    }

    @Override
    public Serializer<T> serializer() {
        return this.inner.serializer();
    }

    @Override
    public Deserializer<T> deserializer() {
        return this.inner.deserializer();
    }

    public void configure(Map<String, ?> serdeConfig, boolean isSerdeForRecordKeys) {
        this.inner.serializer().configure(serdeConfig, isSerdeForRecordKeys);
        this.inner.deserializer().configure(serdeConfig, isSerdeForRecordKeys);
    }

    public void close() {
        this.inner.serializer().close();
        this.inner.deserializer().close();
    }
}
