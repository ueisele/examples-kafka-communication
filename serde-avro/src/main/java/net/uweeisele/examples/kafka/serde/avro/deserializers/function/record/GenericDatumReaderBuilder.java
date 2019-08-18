package net.uweeisele.examples.kafka.serde.avro.deserializers.function.record;

import net.uweeisele.examples.kafka.serde.avro.deserializers.function.record.SchemaAwareDatumReader.Generic;
import net.uweeisele.examples.kafka.serde.avro.deserializers.payload.ReaderSchemaAware;
import net.uweeisele.examples.kafka.serde.avro.deserializers.payload.WriterSchemaAware;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import java.util.function.BiFunction;

public class GenericDatumReaderBuilder<S extends WriterSchemaAware & ReaderSchemaAware> extends RecordDatumReaderBuilder<S, GenericRecord> {

    public GenericDatumReaderBuilder() {
        this(Generic::new);
    }

    protected <T extends GenericDatumReader<GenericRecord> & SchemaAwareDatumReader<GenericRecord>> GenericDatumReaderBuilder(BiFunction<Schema, Schema, T> datumReaderFactory) {
        super(GenericRecord.class, datumReaderFactory);
    }

    public static GenericDatumReaderBuilder genericDatumReaderBuilder() {
        return new GenericDatumReaderBuilder();
    }

    @Override
    protected Schema getReaderSchema(WriterSchemaAware writerSchemaAware) {
        return writerSchemaAware.writerSchema();
    }

}
