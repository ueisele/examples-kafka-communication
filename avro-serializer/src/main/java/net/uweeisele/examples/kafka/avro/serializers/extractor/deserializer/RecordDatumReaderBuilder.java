package net.uweeisele.examples.kafka.avro.serializers.extractor.deserializer;

import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.SchemaCompatibility.SchemaPairCompatibility;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.errors.SerializationException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.avro.SchemaCompatibility.checkReaderWriterCompatibility;

public abstract class RecordDatumReaderBuilder<D extends IndexedRecord> implements DatumReaderBuilder<D> {

    private final Class<D> type;
    private final BiFunction<Schema, Schema, ? extends SchemaAwareDatumReader<D>> datumReaderFactory;

    private final Map<SchemaPair, SchemaPairCompatibility> compatibilityCache = new ConcurrentHashMap<>();

    public RecordDatumReaderBuilder(Class<D> type, BiFunction<Schema, Schema, ? extends SchemaAwareDatumReader<D>> datumReaderFactory) {
        this.type = requireNonNull(type);
        this.datumReaderFactory = requireNonNull(datumReaderFactory);
    }

    @Override
    public SchemaAwareDatumReader<D> build(Schema writerSchema, Schema readerSchema) {
        assertIsTypeRecord(writerSchema);
        Schema actualReaderSchema = readerSchema != null ? readerSchema : getReaderSchema(writerSchema);
        assertWriterAndReaderSchemaCompatible(writerSchema, actualReaderSchema);
        return new TypeSafeDatumReader<>(type, datumReaderFactory.apply(writerSchema, actualReaderSchema));
    }

    private static void assertIsTypeRecord(Schema schema) {
        if (schema == null) {
            throw new SerializationException(format("Schema is null, but expected type %s.", Schema.Type.RECORD.getName()));
        }
        if (!Schema.Type.RECORD.equals(schema.getType())) {
            throw new SerializationException(format("Schema has type %s, but expected type %s.", schema.getType().getName(), Schema.Type.RECORD.getName()));
        }
    }

    protected abstract Schema getReaderSchema(Schema writerSchema);

    private void assertWriterAndReaderSchemaCompatible(Schema writerSchema, Schema readerSchema) {
        SchemaPairCompatibility compatibility =
                compatibilityCache.computeIfAbsent(new SchemaPair(writerSchema, readerSchema), p -> checkReaderWriterCompatibility(p.readerSchema(), p.writerSchema()));
        if (!SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE.equals(compatibility.getType())) {
            throw new SerializationException(compatibility.getDescription());
        }
    }

}
