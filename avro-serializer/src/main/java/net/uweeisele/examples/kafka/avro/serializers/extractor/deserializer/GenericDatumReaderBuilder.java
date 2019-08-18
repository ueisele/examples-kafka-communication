package net.uweeisele.examples.kafka.avro.serializers.extractor.deserializer;

import net.uweeisele.examples.kafka.avro.serializers.extractor.deserializer.SchemaAwareDatumReader.Generic;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import java.util.function.BiFunction;

public class GenericDatumReaderBuilder extends RecordDatumReaderBuilder<GenericRecord> {

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
    protected Schema getReaderSchema(Schema writerSchema) {
        return writerSchema;
    }

}
