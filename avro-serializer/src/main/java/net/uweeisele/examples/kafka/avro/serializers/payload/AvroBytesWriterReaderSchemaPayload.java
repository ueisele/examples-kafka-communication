package net.uweeisele.examples.kafka.avro.serializers.payload;

import org.apache.avro.Schema;

import java.util.function.Consumer;

public class AvroBytesWriterReaderSchemaPayload extends AvroBytesWriterSchemaPayload implements ReaderSchema {


    public <T extends AvroBytes & WriterSchema> AvroBytesWriterReaderSchemaPayload(T payload) {
        super(payload);
    }

    @Override
    public AvroBytesWriterReaderSchemaPayload withReaderSchema(Schema readerSchema) {
        payload.put(ReaderSchema.KEY, readerSchema);
        return this;
    }

    @Override
    public AvroBytesWriterReaderSchemaPayload with(Consumer<Payload> action) {
        action.accept(payload());
        return this;
    }

    @Override
    public Schema readerSchema() {
        return payload.get(ReaderSchema.KEY);
    }
}
