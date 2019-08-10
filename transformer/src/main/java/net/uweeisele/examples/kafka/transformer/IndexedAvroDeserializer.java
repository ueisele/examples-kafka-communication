package net.uweeisele.examples.kafka.transformer;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class IndexedAvroDeserializer<T extends IndexedRecord> implements Deserializer<T> {

    private final KafkaAvroDeserializer inner;

    public IndexedAvroDeserializer() {
        this.inner = new KafkaAvroDeserializer();
    }

    IndexedAvroDeserializer(SchemaRegistryClient client) {
        this.inner = new KafkaAvroDeserializer(client);
    }

    public void configure(Map<String, ?> deserializerConfig, boolean isDeserializerForRecordKeys) {
        this.inner.configure(deserializerConfig, isDeserializerForRecordKeys);
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        return (T)this.inner.deserialize(topic, bytes);
    }

    public void close() {
        this.inner.close();
    }

}
