package net.uweeisele.examples.kafka.avro.serializers;

import com.fasterxml.jackson.core.JsonParseException;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDe;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

import static net.uweeisele.examples.kafka.avro.serializers.AvroSchemaUtils.getPrimitiveSchemas;

public class KafkaAvroDeserializer extends AbstractKafkaAvroSerDe implements Deserializer<IndexedRecord> {

    private final Predicate<Schema> knownSchemaPredicate;
    private final Predicate<Schema> deserializablePredicate;

    private boolean isKey;

    private final DecoderFactory decoderFactory = DecoderFactory.get();
    protected boolean useSpecificAvroReader = false;
    private final Map<String, Schema> readerSchemaCache = new ConcurrentHashMap<String, Schema>();

    public KafkaAvroDeserializer(Predicate<Schema> knownSchemaPredicate, Predicate<Schema> deserializablePredicate) {
        this(knownSchemaPredicate, deserializablePredicate, null);
    }

    public KafkaAvroDeserializer(Predicate<Schema> knownSchemaPredicate, Predicate<Schema> deserializablePredicate, SchemaRegistryClient client) {
        this.knownSchemaPredicate = knownSchemaPredicate;
        this.deserializablePredicate = deserializablePredicate;
        this.schemaRegistry = client;    }

    public KafkaAvroDeserializer(Predicate<Schema> knownSchemaPredicate, Predicate<Schema> deserializablePredicate, SchemaRegistryClient client, Map<String, ?> props) {
        this.knownSchemaPredicate = knownSchemaPredicate;
        this.deserializablePredicate = deserializablePredicate;
        this.schemaRegistry = client;
        this.configure(new KafkaAvroDeserializerConfig(props));
    }

    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        this.configure(new KafkaAvroDeserializerConfig(configs));
    }

    public IndexedRecord deserialize(String s, byte[] bytes) {
        return this.deserialize(bytes);
    }

    public IndexedRecord deserialize(String s, byte[] bytes, Schema readerSchema) {
        return this.deserialize(bytes, readerSchema);
    }

    /**
     * Sets properties for this deserializer without overriding the schema registry client itself.
     * Useful for testing, where a mock client is injected.
     */
    protected void configure(KafkaAvroDeserializerConfig config) {
        configureClientProperties(config);
        useSpecificAvroReader = config
                .getBoolean(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG);
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

    /**
     * Deserializes the payload without including schema information for primitive types, maps, and
     * arrays. Just the resulting deserialized object is returned.
     *
     * <p>This behavior is the norm for Decoders/Deserializers.
     *
     * @param payload serialized data
     * @return the deserialized object
     */
    protected IndexedRecord deserialize(byte[] payload) throws SerializationException {
        return deserialize(payload, null);
    }

    /**
     * Just like single-parameter version but accepts an Avro schema to use for reading
     *
     * @param payload      serialized data
     * @param readerSchema schema to use for Avro read (optional, enables Avro projection)
     * @return the deserialized object
     */
    protected IndexedRecord deserialize(byte[] payload, Schema readerSchema) throws SerializationException {
        if (payload == null) {
            return null;
        }

        int id = -1;
        try {
            ByteBuffer buffer = getAvroByteBuffer(payload);
            id = buffer.getInt();
            Schema writerSchema = getWriterSchema(id);
            if (!knownSchemaPredicate.test(writerSchema)) {
                // throw Unknown Schema Exception (not retryable)
                return null;
            } else if (!deserializablePredicate.test(writerSchema)) {
                // throw Unsupported Known Schema Exception (not retryable)
                return null;
            } else {
                int length = buffer.limit() - 1 - idSize;
                final Object result;
                if (writerSchema.getType().equals(Schema.Type.BYTES)) {
                    byte[] bytes = new byte[length];
                    buffer.get(bytes, 0, length);
                    result = bytes;
                } else {
                    int start = buffer.position() + buffer.arrayOffset();
                    DatumReader<? extends IndexedRecord> reader = getDatumReader(writerSchema, readerSchema);
                    Object
                            object =
                            reader.read(null, decoderFactory.binaryDecoder(buffer.array(), start, length, null));

                    if (writerSchema.getType().equals(Schema.Type.STRING)) {
                        object = object.toString(); // Utf8 -> String
                    }
                    result = object;
                }

                return (IndexedRecord) result;
            }
        } catch (IOException | RuntimeException e) {
            // avro deserialization may throw AvroRuntimeException, NullPointerException, etc
            throw new SerializationException("Error deserializing Avro message for id " + id, e);
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

    private DatumReader<? extends IndexedRecord> getDatumReader(Schema writerSchema, Schema readerSchema) {
        boolean writerSchemaIsPrimitive = getPrimitiveSchemas().containsValue(writerSchema);
        // do not use SpecificDatumReader if writerSchema is a primitive
        if (useSpecificAvroReader && !writerSchemaIsPrimitive) {
            if (readerSchema == null) {
                readerSchema = getReaderSchema(writerSchema);
            }
            return new SpecificDatumReader<>(writerSchema, readerSchema);
        } else {
            if (readerSchema == null) {
                return new GenericDatumReader<>(writerSchema);
            }
            return new GenericDatumReader<>(writerSchema, readerSchema);
        }
    }

    private Schema getReaderSchema(Schema writerSchema) {
        Schema readerSchema = readerSchemaCache.get(writerSchema.getFullName());
        if (readerSchema == null) {
            Class<? extends SpecificRecord> readerClass = SpecificData.get().getClass(writerSchema);
            if (readerClass != null) {
                try {
                    readerSchema = readerClass.newInstance().getSchema();
                } catch (InstantiationException e) {
                    throw new SerializationException(writerSchema.getFullName()
                            + " specified by the "
                            + "writers schema could not be instantiated to "
                            + "find the readers schema.");
                } catch (IllegalAccessException e) {
                    throw new SerializationException(writerSchema.getFullName()
                            + " specified by the "
                            + "writers schema is not allowed to be instantiated "
                            + "to find the readers schema.");
                }
                readerSchemaCache.put(writerSchema.getFullName(), readerSchema);
            } else {
                throw new SerializationException("Could not find class "
                        + writerSchema.getFullName()
                        + " specified in writer's schema whilst finding reader's "
                        + "schema for a SpecificRecord.");
            }
        }
        return readerSchema;
    }

}