package net.uweeisele.examples.kafka.serde.avro.protocol.avro.reader;

import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.SchemaCompatibility.SchemaPairCompatibility;
import org.apache.kafka.common.errors.SerializationException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;
import static org.apache.avro.SchemaCompatibility.checkReaderWriterCompatibility;

public class ValidatingDatumReaderBuilder<D> implements DatumReaderBuilder<D> {

    private final DatumReaderBuilder<D> datumReaderBuilder;

    private final Map<SchemaPair, SchemaPairCompatibility> compatibilityCache;

    public ValidatingDatumReaderBuilder(DatumReaderBuilder<D> datumReaderBuilder) {
        this(datumReaderBuilder, new ConcurrentHashMap<>());
    }

    ValidatingDatumReaderBuilder(DatumReaderBuilder<D> datumReaderBuilder, Map<SchemaPair, SchemaPairCompatibility> compatibilityCache) {
        this.datumReaderBuilder = requireNonNull(datumReaderBuilder);
        this.compatibilityCache = requireNonNull(compatibilityCache);
    }

    @Override
    public SchemaAwareDatumReader<D> build(Schema writerSchema, Schema readerSchema) {
        assertWriterAndReaderSchemaCompatible(writerSchema, readerSchema);
        return datumReaderBuilder.build(writerSchema, readerSchema);
    }

    private void assertWriterAndReaderSchemaCompatible(Schema writerSchema, Schema readerSchema) {
        SchemaPairCompatibility compatibility =
                compatibilityCache.computeIfAbsent(new SchemaPair(writerSchema, readerSchema), p -> checkReaderWriterCompatibility(p.readerSchema(), p.writerSchema()));
        if (!SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE.equals(compatibility.getType())) {
            throw new SerializationException(compatibility.getDescription());
        }
    }
}
