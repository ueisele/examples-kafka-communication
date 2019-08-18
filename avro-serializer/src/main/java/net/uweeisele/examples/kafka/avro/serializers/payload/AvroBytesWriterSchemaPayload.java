package net.uweeisele.examples.kafka.avro.serializers.payload;

import org.apache.avro.Schema;

import java.util.function.Consumer;

public class AvroBytesWriterSchemaPayload extends AvroBytesPayload implements WriterSchema {


    public AvroBytesWriterSchemaPayload(AvroBytes payload) {
        super(payload.payload());
    }

    @Override
    public AvroBytesWriterSchemaPayload withWriterSchema(Schema writerSchema) {
        payload.put(WriterSchema.KEY, writerSchema);
        return this;
    }

    @Override
    public AvroBytesWriterSchemaPayload with(Consumer<Payload> action) {
        action.accept(payload());
        return this;
    }

    @Override
    public Schema writerSchema() {
        return payload.get(WriterSchema.KEY);
    }
}
