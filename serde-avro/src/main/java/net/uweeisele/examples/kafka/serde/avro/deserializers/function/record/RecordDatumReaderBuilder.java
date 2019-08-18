package net.uweeisele.examples.kafka.serde.avro.deserializers.function.record;

import net.uweeisele.examples.kafka.serde.avro.deserializers.payload.ReaderSchemaAware;
import net.uweeisele.examples.kafka.serde.avro.deserializers.payload.WriterSchemaAware;
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

public abstract class RecordDatumReaderBuilder<S extends WriterSchemaAware & ReaderSchemaAware, D extends IndexedRecord> implements DatumReaderBuilder<S, D> {

    private final Class<? extends D> type;
    private final BiFunction<Schema, Schema, ? extends SchemaAwareDatumReader<D>> datumReaderFactory;

    private final Map<SchemaPair, SchemaPairCompatibility> compatibilityCache = new ConcurrentHashMap<>();

    public RecordDatumReaderBuilder(Class<? extends D> type, BiFunction<Schema, Schema, ? extends SchemaAwareDatumReader<D>> datumReaderFactory) {
        this.type = requireNonNull(type);
        this.datumReaderFactory = requireNonNull(datumReaderFactory);
    }

    @Override
    public SchemaAwareDatumReader<D> build(S payload) {
        assertIsTypeRecord(payload.writerSchema());
        payload.computeReaderSchemaIfAbsent(() -> getReaderSchema(payload));
        assertWriterAndReaderSchemaCompatible(payload);
        return new TypeSafeDatumReader<>(type, datumReaderFactory.apply(payload.writerSchema(), payload.readerSchema()));
    }

    private static void assertIsTypeRecord(Schema schema) {
        if (schema == null) {
            throw new SerializationException(format("Schema is null, but expected type %s.", Schema.Type.RECORD.getName()));
        }
        if (!Schema.Type.RECORD.equals(schema.getType())) {
            throw new SerializationException(format("Schema has type %s, but expected type %s.", schema.getType().getName(), Schema.Type.RECORD.getName()));
        }
    }

    protected abstract Schema getReaderSchema(WriterSchemaAware payload);

    private void assertWriterAndReaderSchemaCompatible(S payload) {
        SchemaPairCompatibility compatibility =
                compatibilityCache.computeIfAbsent(new SchemaPair(payload.writerSchema(), payload.readerSchema()), p -> checkReaderWriterCompatibility(p.readerSchema(), p.writerSchema()));
        if (!SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE.equals(compatibility.getType())) {
            throw new SerializationException(compatibility.getDescription());
        }
    }

}
