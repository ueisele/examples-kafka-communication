package net.uweeisele.examples.kafka.serde.avro.deserializers.function.record;

import net.uweeisele.examples.kafka.serde.avro.deserializers.payload.ReaderSchemaAware;
import net.uweeisele.examples.kafka.serde.avro.deserializers.payload.WriterSchemaAware;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

public class SpecificDatumReaderBuilder<S extends WriterSchemaAware & ReaderSchemaAware, D extends SpecificRecord> extends RecordDatumReaderBuilder<S, D> {

    private final Map<String, Schema> readerSchemaCache = new ConcurrentHashMap<>();

    public SpecificDatumReaderBuilder(Class<? extends D> type) {
        this(type, SchemaAwareDatumReader.Specific::new);
    }

    protected  <T extends GenericDatumReader<D> & SchemaAwareDatumReader<D>> SpecificDatumReaderBuilder(Class<? extends D> type, BiFunction<Schema, Schema, T> datumReaderFactory) {
        super(type, datumReaderFactory);
    }

    public static <S extends WriterSchemaAware & ReaderSchemaAware> SpecificDatumReaderBuilder<S, ? extends SpecificRecord> specificDatumReaderBuilder() {
        return new SpecificDatumReaderBuilder<>(SpecificRecord.class);
    }

    @Override
    protected Schema getReaderSchema(WriterSchemaAware writerSchemaAware) {
        Schema writerSchema = writerSchemaAware.writerSchema();
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
