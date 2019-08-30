package net.uweeisele.examples.kafka.serde.avro.protocol.avro.schema;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class ConfluentAvroSchemaRegistryClient implements AvroSchemaRegistryClient {

    private final SchemaRegistryClient client;

    public ConfluentAvroSchemaRegistryClient(SchemaRegistryClient client) {
        this.client = requireNonNull(client);
    }

    @Override
    public int register(String subject, Schema schema) {
        try {
            return client.register(subject, schema);
        } catch (IOException | RestClientException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getId(String subject, Schema schema) {
        try {
            return client.getId(subject, schema);
        } catch (IOException | RestClientException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Schema getById(int id) {
        try {
            return client.getById(id);
        } catch (IOException | RestClientException e) {
            throw new RuntimeException(e);
        }
    }

    SchemaRegistryClient getClient() {
        return client;
    }
}
