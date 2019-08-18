package net.uweeisele.examples.kafka.serde.avro.deserializers;

import com.fasterxml.jackson.core.JsonParseException;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Predicate;

public class FilterKafkaAvroDeserializer extends KafkaAvroDeserializer {

    private final Predicate<Schema> knownSchemaPredicate;
    private final Predicate<Schema> deserializablePredicate;

    public FilterKafkaAvroDeserializer(Predicate<Schema> knownSchemaPredicate, Predicate<Schema> deserializablePredicate) {
        this.knownSchemaPredicate = knownSchemaPredicate;
        this.deserializablePredicate = deserializablePredicate;
    }

    public FilterKafkaAvroDeserializer(Predicate<Schema> knownSchemaPredicate, Predicate<Schema> deserializablePredicate, SchemaRegistryClient client) {
        super(client);
        this.knownSchemaPredicate = knownSchemaPredicate;
        this.deserializablePredicate = deserializablePredicate;    }

    public FilterKafkaAvroDeserializer(Predicate<Schema> knownSchemaPredicate, Predicate<Schema> deserializablePredicate, SchemaRegistryClient client, Map<String, ?> props) {
        super(client, props);
        this.knownSchemaPredicate = knownSchemaPredicate;
        this.deserializablePredicate = deserializablePredicate;
    }

    @Override
    public IndexedRecord deserialize(String s, byte[] bytes) {
        return (IndexedRecord) super.deserialize(s, bytes);
    }

    @Override
    public IndexedRecord deserialize(String s, byte[] bytes, Schema readerSchema) {
        return (IndexedRecord) super.deserialize(s, bytes, readerSchema);
    }

    @Override
    protected Object deserialize(boolean includeSchemaAndVersion, String topic, Boolean isKey, byte[] payload, Schema readerSchema) throws SerializationException {
        if (payload == null) {
            return null;
        } else {
            ByteBuffer buffer = this.getAvroByteBuffer(payload);
            int id = buffer.getInt();
            Schema writerSchema = getWriterSchema(id);
            if (!knownSchemaPredicate.test(writerSchema)) {
                // throw Unknown Schema Exception (not retryable)
            } else if (!deserializablePredicate.test(writerSchema)) {
                // throw Unsupported Known Schema Exception (not retryable)
            } else {
                return super.deserialize(includeSchemaAndVersion, topic, isKey, payload, readerSchema);
            }
        }
        return null;
    }

    private ByteBuffer getAvroByteBuffer(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        if (buffer.get() != 0) {
            // throw Invalid Format Exception (No Avro) -> Not Retryable
            throw new SerializationException("Unknown magic byte!");
        } else {
            return buffer;
        }
    }

    // Fragestellung: wann liegt ein Problem an den Daten und wann an einem externen System
    // -> externes System: Retry
    // > daten: verwerfen oder dead letter queue
    private Schema getWriterSchema(int id) {
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