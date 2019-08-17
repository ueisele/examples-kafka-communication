package net.uweeisele.examples.kafka.avro.serializers.payload;

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
    public Integer writerSchemaId() {
        return payload.get(WriterSchemaId.KEY);
    }
}
