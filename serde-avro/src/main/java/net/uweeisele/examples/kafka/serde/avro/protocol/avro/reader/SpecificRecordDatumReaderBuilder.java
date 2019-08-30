package net.uweeisele.examples.kafka.serde.avro.protocol.avro.reader;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class SpecificRecordDatumReaderBuilder<D extends SpecificRecord> extends RecordDatumReaderBuilder<D> {

    private final Map<String, Schema> readerSchemaCache;

    public SpecificRecordDatumReaderBuilder() {
        this(identity());
    }

    public SpecificRecordDatumReaderBuilder(Function<DatumReaderBuilder<D>, DatumReaderBuilder<D>> datumReaderBuilder) {
        this(datumReaderBuilder, new ConcurrentHashMap<>());
    }

    SpecificRecordDatumReaderBuilder(Function<DatumReaderBuilder<D>, DatumReaderBuilder<D>> datumReaderBuilder, Map<String, Schema> readerSchemaCache) {
        super(datumReaderBuilder.apply(Specific::new));
        this.readerSchemaCache = requireNonNull(readerSchemaCache);
    }

    @Override
    protected Schema resolveReaderSchemaByWriterSchema(Schema writerSchema) {
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

    private static class Specific<D extends SpecificRecord> extends SpecificDatumReader<D> implements SchemaAwareDatumReader<D>  {
        public Specific(Schema writer, Schema reader) {
            super(writer, reader);
        }
        @Override
        public String toString() {
            return "SpecificDatumReader";
        }
    }

}
