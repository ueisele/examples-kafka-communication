package net.uweeisele.examples.kafka.avro.serializers.payload;

import org.apache.avro.Schema;

public class AvroBytesWriterReaderSchemaPayload<T extends AvroBytes & WriterSchema> extends AvroBytesWriterSchemaPayload implements ReaderSchema {


    public AvroBytesWriterReaderSchemaPayload(T payload) {
        super(payload);
    }

    @Override
    public AvroBytesWriterReaderSchemaPayload withReaderSchema(Schema readerSchema) {
        payload.put(ReaderSchema.KEY, readerSchema);
        return this;
    }

    @Override
    public Schema readerSchema() {
        return payload.get(ReaderSchema.KEY);
    }
}
