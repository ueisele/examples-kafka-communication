package net.uweeisele.examples.kafka.avro.serializers.extractor.deserializer;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

public class SpecificDatumReaderBuilder<D extends SpecificRecord> extends RecordDatumReaderBuilder<D> {

    private final Map<String, Schema> readerSchemaCache = new ConcurrentHashMap<>();

    public SpecificDatumReaderBuilder(Class<D> type) {
        this(type, SpecificDatumReader::new);
    }

    protected SpecificDatumReaderBuilder(Class<D> type, BiFunction<Schema, Schema, SpecificDatumReader<D>> datumReaderFactory) {
        super(type, datumReaderFactory);
    }

    public SpecificDatumReaderBuilder<SpecificRecord> specificDatumReaderBuilder() {
        return new SpecificDatumReaderBuilder<>(SpecificRecord.class);
    }

    @Override
    protected Schema getReaderSchema(Schema writerSchema) {
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
        return readerSchema;    }

}
