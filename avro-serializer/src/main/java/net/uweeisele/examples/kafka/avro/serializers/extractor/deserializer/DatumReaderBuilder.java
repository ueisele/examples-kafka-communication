package net.uweeisele.examples.kafka.avro.serializers.extractor.deserializer;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;

import java.util.function.BiFunction;

public interface DatumReaderBuilder<D> extends BiFunction<Schema, Schema, SchemaAwareDatumReader<D>> {

    default DatumReader<D> build(Schema writerSchema) {
        return build(writerSchema, null);
    }

    @Override
    default SchemaAwareDatumReader<D> apply(Schema writerSchema, Schema readerSchema) {
        return build(writerSchema, readerSchema);
    }

    SchemaAwareDatumReader<D> build(Schema writerSchema, Schema readerSchema);

}
