package net.uweeisele.examples.kafka.serde.avro.deserializers.function.schema;

import com.fasterxml.jackson.core.JsonParseException;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import net.uweeisele.examples.kafka.serde.avro.builder.SchemaRegistryClientBuilder;
import net.uweeisele.examples.kafka.serde.avro.deserializers.payload.WriterSchemaAware;
import net.uweeisele.examples.kafka.serde.avro.function.Configurable;
import net.uweeisele.examples.kafka.serde.avro.function.ConfigurableFunction;
import net.uweeisele.examples.kafka.serde.avro.deserializers.payload.PayloadAware;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class SchemaRegistryWriterDeserializer<S extends PayloadAware, R extends WriterSchemaAware> implements ConfigurableFunction<S, R> {

    private static final String KEY_WRITER_SCHEMA_ID = "avro.writer.schema.id";

    private final Function<? super S, Integer> schemaIdDeserializer;
    private final Function<Properties, ? extends SchemaRegistryClient> schemaRegistryClientBuilder;
    private final Function<? super S, ? extends R> payloadFactory;

    private SchemaRegistryClient schemaRegistry;

    public SchemaRegistryWriterDeserializer(Function<? super S, Integer> schemaIdDeserializer, Function<? super S, ? extends R> payloadFactory) {
        this(schemaIdDeserializer, new SchemaRegistryClientBuilder(), payloadFactory);
    }

    public SchemaRegistryWriterDeserializer(Function<? super S, Integer> schemaIdDeserializer, Function<Properties, ? extends SchemaRegistryClient> schemaRegistryClientBuilder, Function<? super S, ? extends R> payloadFactory) {
        this.schemaIdDeserializer = requireNonNull(schemaIdDeserializer);
        this.schemaRegistryClientBuilder = requireNonNull(schemaRegistryClientBuilder);
        this.payloadFactory = requireNonNull(payloadFactory);
    }

    @Override
    public SchemaRegistryWriterDeserializer<S, R> configure(Properties properties) {
        if (schemaIdDeserializer instanceof Configurable) {
            ((Configurable) schemaIdDeserializer).configure(properties);
        }
        schemaRegistry = schemaRegistryClientBuilder.apply(properties);
        if (payloadFactory instanceof Configurable) {
            ((Configurable) payloadFactory).configure(properties);
        }
        return this;
    }

    @Override
    public R apply(S payload) throws SerializationException {
        R enrichedPayload = payloadFactory.apply(payload);
        Integer schemaId = schemaIdDeserializer.apply(payload);
        enrichedPayload.withAttribute(KEY_WRITER_SCHEMA_ID, String.valueOf(schemaId));
        Schema schema = getSchema(schemaId);
        enrichedPayload.withWriterSchema(schema);
        return enrichedPayload;
    }

    // Fragestellung: wann liegt ein Problem an den Daten und wann an einem externen System
    // -> externes System: Retry
    // > daten: verwerfen oder dead letter queue
    private Schema getSchema(int id) throws SerializationException {
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
