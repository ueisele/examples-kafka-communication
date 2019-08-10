package net.uweeisele.examples.kafka.transformer;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Predicate;

public class FilteringKafkaAvroDeserializer extends KafkaAvroDeserializer {

    private final Predicate<Schema> knownSchemaPredicate;
    private final Predicate<Schema> deserializablePredicate;

    public FilteringKafkaAvroDeserializer(Predicate<Schema> knownSchemaPredicate, Predicate<Schema> deserializablePredicate) {
        this.knownSchemaPredicate = knownSchemaPredicate;
        this.deserializablePredicate = deserializablePredicate;
    }

    public FilteringKafkaAvroDeserializer(Predicate<Schema> knownSchemaPredicate, Predicate<Schema> deserializablePredicate, SchemaRegistryClient client) {
        super(client);
        this.knownSchemaPredicate = knownSchemaPredicate;
        this.deserializablePredicate = deserializablePredicate;    }

    public FilteringKafkaAvroDeserializer(Predicate<Schema> knownSchemaPredicate, Predicate<Schema> deserializablePredicate, SchemaRegistryClient client, Map<String, ?> props) {
        super(client, props);
        this.knownSchemaPredicate = knownSchemaPredicate;
        this.deserializablePredicate = deserializablePredicate;
    }

    @Override
    protected Object deserialize(boolean includeSchemaAndVersion, String topic, Boolean isKey, byte[] payload, Schema readerSchema) throws SerializationException {
        if (payload == null) {
            return null;
        } else {
            ByteBuffer buffer = this.getAvroByteBuffer(payload);
            int id = buffer.getInt();
            Schema schema = getAvroSchema(id);
            if (!knownSchemaPredicate.test(schema)) {
                // throw Unknown Schema Exception (not retryable)
            } else if (!deserializablePredicate.test(schema)) {
                // throw Unsupported Known Schema Exception (not retryable)
            } else {
                return super.deserialize(includeSchemaAndVersion, topic, isKey, payload, readerSchema);
            }
        }
    }

    private ByteBuffer getAvroByteBuffer(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        if (buffer.get() != 0) {
            // throw Invalid Format Exception (No Avro)
            throw new SerializationException("Unknown magic byte!");
        } else {
            return buffer;
        }
    }

    private Schema getAvroSchema(int id) {
        try {
            return this.schemaRegistry.getById(id);
        } catch (RestClientException | IOException e) {
            // throw Retryable Client Exception
            throw new SerializationException("Error retrieving Avro schema for id " + id, e);
        }
    }
}