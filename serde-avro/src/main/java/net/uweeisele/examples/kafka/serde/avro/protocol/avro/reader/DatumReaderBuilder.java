package net.uweeisele.examples.kafka.serde.avro.protocol.avro.reader;

import org.apache.avro.Schema;

@FunctionalInterface
public interface DatumReaderBuilder<D> {

    default SchemaAwareDatumReader<D> build(Schema writerSchema) {
        return build(writerSchema, null);
    }

    SchemaAwareDatumReader<D> build(Schema writerSchema, Schema readerSchema);
}
