package net.uweeisele.examples.kafka.avro.serializers.extractor;

import com.fasterxml.jackson.core.JsonParseException;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import net.uweeisele.examples.kafka.avro.serializers.builder.SchemaRegistryClientBuilder;
import net.uweeisele.examples.kafka.avro.serializers.payload.AvroBytes;
import net.uweeisele.examples.kafka.avro.serializers.payload.AvroBytesWriterSchemaPayload;
import net.uweeisele.examples.kafka.avro.serializers.payload.Payload.Key;
import net.uweeisele.examples.kafka.avro.serializers.payload.WriterSchemaId;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

public class SchemaRegistryWriterSchemaPayloadExtractor<S extends AvroBytes & WriterSchemaId> implements PayloadExtractor<S, AvroBytesWriterSchemaPayload> {

    private static final Key<String> KEY_SCHEMA_REGISTRY_URL = new Key<>("SchemaRegistryUrl");

    private final SchemaRegistryClientBuilder schemaRegistryClientBuilder;

    private SchemaRegistryClient schemaRegistry;
    private String schemaRegistryUrls;

    public SchemaRegistryWriterSchemaPayloadExtractor(Properties properties) {
        this();
        configure(properties);
    }

    public SchemaRegistryWriterSchemaPayloadExtractor() {
        this(new SchemaRegistryClientBuilder());
    }

    public SchemaRegistryWriterSchemaPayloadExtractor(SchemaRegistryClientBuilder schemaRegistryClientBuilder, Properties properties) {
        this.schemaRegistryClientBuilder = requireNonNull(schemaRegistryClientBuilder);
        configure(properties);
    }

    public SchemaRegistryWriterSchemaPayloadExtractor(SchemaRegistryClientBuilder schemaRegistryClientBuilder) {
        this.schemaRegistryClientBuilder = requireNonNull(schemaRegistryClientBuilder);
    }

    @Override
    public void configure(Properties properties) {
        schemaRegistry = schemaRegistryClientBuilder.apply(properties);
        schemaRegistryUrls = schemaRegistryClientBuilder.getSchemaRegistryUrls(properties);
    }

    @Override
    public AvroBytesWriterSchemaPayload extract(S payload) throws SerializationException {
        return new AvroBytesWriterSchemaPayload(payload)
                .withWriterSchema(getWriterSchema(payload.writerSchemaId()))
                .with(p -> p.put(KEY_SCHEMA_REGISTRY_URL, schemaRegistryUrls));
    }

    // Fragestellung: wann liegt ein Problem an den Daten und wann an einem externen System
    // -> externes System: Retry
    // > daten: verwerfen oder dead letter queue
    private Schema getWriterSchema(int id) throws SerializationException {
        try {
            return this.schemaRegistry.getById(id);
        } catch (SchemaParseException e) {
            //-> Not Retryable
            throw new SerializationException("Error retrieving Avro schema for id " + id, e);
        } catch (JsonParseException e) {
            //-> Not Retryable
            throw new SerializationException("Error retrieving Avro schema for id " + id, e);
        } catch (SocketTimeoutException e) {
            // RestClient Read Timeout (IOException)-> Retryable
            throw new SerializationException("Error retrieving Avro schema for id " + id, e);
        } catch (UnknownHostException e) {
            // RestClient Unknown Host (IOException)-> Retryable (vieleicht)
            throw new SerializationException("Error retrieving Avro schema for id " + id, e);
        } catch (ConnectException e) {
            // RestClient Connection refused (IOException)-> Retryable (vieleicht)
            throw new SerializationException("Error retrieving Avro schema for id " + id, e);
        } catch (SocketException e) {
            // SocketException: Connection reset
            // RestClient Connection reset (port offen, aber falscher service) (IOException)-> Retryable (vieleicht)
            throw new SerializationException("Error retrieving Avro schema for id " + id, e);
        } catch (IOException e) {
            //-> Not Retryable
            // Wenn von RestClient dann möglicherweise Retryable // Kommt aber auch vom Parser, dann nicht retryable
            throw new SerializationException("Error retrieving Avro schema for id " + id, e);
        } catch (RestClientException e) {
            // Retryable oder nicht?
            // RestClientException: Schema not found; error code: 40403
            // Auch wenn antwort nicht 200 ist -> test was bei z.B. 500 passiert --> wäre Retryable
            throw new SerializationException("Error retrieving Avro schema for id " + id, e);
        }
    }
}
