package net.uweeisele.examples.kafka.serde.avro.protocol.avro.reader;

import org.apache.avro.Schema;

import static java.util.Objects.requireNonNull;

public class TypeSafeDatumReaderBuilder<D> implements DatumReaderBuilder<D> {

    private final Class<? extends D> type;

    private final DatumReaderBuilder<D> datumReaderBuilder;

    public TypeSafeDatumReaderBuilder(Class<? extends D> type, DatumReaderBuilder<D> datumReaderBuilder) {
        this.type = requireNonNull(type);
        this.datumReaderBuilder = requireNonNull(datumReaderBuilder);
    }

    @Override
    public SchemaAwareDatumReader<D> build(Schema writerSchema, Schema readerSchema) {
        return new TypeSafeDatumReader<>(type, datumReaderBuilder.build(writerSchema, readerSchema));
    }
}
