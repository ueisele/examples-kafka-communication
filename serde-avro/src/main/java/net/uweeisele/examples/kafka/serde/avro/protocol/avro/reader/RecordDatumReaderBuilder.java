package net.uweeisele.examples.kafka.serde.avro.protocol.avro.reader;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.errors.SerializationException;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public abstract class RecordDatumReaderBuilder<D extends IndexedRecord> implements DatumReaderBuilder<D> {

    private final DatumReaderBuilder<D> datumReaderBuilder;

    private Schema readerSchema;

    public RecordDatumReaderBuilder(DatumReaderBuilder<D> datumReaderBuilder) {
        this.datumReaderBuilder = requireNonNull(datumReaderBuilder);
    }

    @Override
    public SchemaAwareDatumReader<D> build(Schema writerSchema, Schema readerSchema) {
        assertIsTypeRecord(writerSchema);
        Schema actualReaderSchema = resolveActualReaderSchema(writerSchema, readerSchema);
        return datumReaderBuilder.build(writerSchema, actualReaderSchema);
    }

    public RecordDatumReaderBuilder<D> withReaderSchema(Schema readerSchema) {
        this.readerSchema = readerSchema;
        return this;
    }

    private static void assertIsTypeRecord(Schema schema) {
        if (schema == null) {
            throw new SerializationException(format("Schema is null, but expected type %s.", Schema.Type.RECORD.getName()));
        }
        if (!Schema.Type.RECORD.equals(schema.getType())) {
            throw new SerializationException(format("Schema has type %s, but expected type %s.", schema.getType().getName(), Schema.Type.RECORD.getName()));
        }
    }

    private Schema resolveActualReaderSchema(Schema writerSchema, Schema readerSchema) {
        if (readerSchema != null) {
            return readerSchema;
        } else if (this.readerSchema != null) {
            return this.readerSchema;
        } else {
            return resolveReaderSchemaByWriterSchema(writerSchema);
        }
    }

    protected abstract Schema resolveReaderSchemaByWriterSchema(Schema writerSchema);


}
