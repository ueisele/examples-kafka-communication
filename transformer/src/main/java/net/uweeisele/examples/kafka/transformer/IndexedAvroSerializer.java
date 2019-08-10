package net.uweeisele.examples.kafka.transformer;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class IndexedAvroSerializer<T extends IndexedRecord> implements Serializer<T> {

    private final KafkaAvroSerializer inner;

    public IndexedAvroSerializer() {
        this.inner = new KafkaAvroSerializer();
    }

    IndexedAvroSerializer(SchemaRegistryClient client) {
        this.inner = new KafkaAvroSerializer(client);
    }

    public void configure(Map<String, ?> serializerConfig, boolean isSerializerForRecordKeys) {
        this.inner.configure(serializerConfig, isSerializerForRecordKeys);
    }

    @Override
    public byte[] serialize(String topic, T record) {
        return this.inner.serialize(topic, record);
    }

    public void close() {
        this.inner.close();
    }
}
