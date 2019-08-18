package net.uweeisele.examples.kafka.serde.avro.deserializers.payload;

import org.apache.avro.Schema;

import java.util.function.Supplier;

public class AvroDeserializablePayload extends DeserializablePayload implements WriterSchemaAware, ReaderSchemaAware {

    private static final Key<Schema> KEY_WRITER_SCHEMA = new Key<>("payload.deserializable.avro.writer.schema");
    private static final Key<Schema> KEY_READER_SCHEMA = new Key<>("payload.deserializable.avro.reader.schema");

    public AvroDeserializablePayload() {
    }

    public AvroDeserializablePayload(PayloadAware payloadAware) {
        super(payloadAware);
    }

    public AvroDeserializablePayload(Payload payload) {
        super(payload);
    }

    @Override
    public Schema writerSchema() {
        return get(KEY_WRITER_SCHEMA);
    }

    @Override
    public AvroDeserializablePayload withWriterSchema(Schema writerSchema) {
        put(KEY_WRITER_SCHEMA, writerSchema);
        return this;
    }

    @Override
    public Schema computeWriterSchemaIfAbsent(Supplier<Schema> supplier) {
        return computeIfAbsent(KEY_WRITER_SCHEMA, supplier);
    }

    @Override
    public Schema readerSchema() {
        return get(KEY_READER_SCHEMA);
    }

    @Override
    public AvroDeserializablePayload withReaderSchema(Schema readerSchema) {
        put(KEY_READER_SCHEMA, readerSchema);
        return this;
    }

    @Override
    public Schema computeReaderSchemaIfAbsent(Supplier<Schema> supplier) {
        return computeIfAbsent(KEY_READER_SCHEMA, supplier);
    }
}
