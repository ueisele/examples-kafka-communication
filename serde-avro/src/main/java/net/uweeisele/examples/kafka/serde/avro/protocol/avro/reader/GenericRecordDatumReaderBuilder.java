package net.uweeisele.examples.kafka.serde.avro.protocol.avro.reader;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import java.util.function.Function;

import static java.util.function.Function.identity;

public class GenericRecordDatumReaderBuilder extends RecordDatumReaderBuilder<GenericRecord> {

    public GenericRecordDatumReaderBuilder() {
        this(identity());
    }

    public GenericRecordDatumReaderBuilder(Function<DatumReaderBuilder<GenericRecord>, DatumReaderBuilder<GenericRecord>> datumReaderBuilder) {
        super(datumReaderBuilder.apply(GenericRecordDatumReaderBuilder.Generic::new));
    }

    @Override
    protected Schema resolveReaderSchemaByWriterSchema(Schema writerSchema) {
        return writerSchema;
    }

    private static class Generic extends GenericDatumReader<GenericRecord> implements SchemaAwareDatumReader<GenericRecord> {
        public Generic(Schema writer, Schema reader) {
            super(writer, reader);
        }
        @Override
        public String toString() {
            return "GenericDatumReader";
        }
    }
}
