package net.uweeisele.examples.kafka.avro.serializers.payload;

import java.util.function.Consumer;

public class AvroBytesWriterSchemaIdPayload extends AvroBytesPayload implements WriterSchemaId {

    public AvroBytesWriterSchemaIdPayload(AvroBytes payload) {
        super(payload.payload());
    }

    @Override
    public AvroBytesWriterSchemaIdPayload withWriterSchemaId(Integer writerSchemaId) {
        payload.put(WriterSchemaId.KEY, writerSchemaId);
        return this;
    }

    @Override
    public AvroBytesWriterSchemaIdPayload with(Consumer<Payload> action) {
        action.accept(payload());
        return this;
    }

    @Override
    public Integer writerSchemaId() {
        return payload.get(WriterSchemaId.KEY);
    }
}
