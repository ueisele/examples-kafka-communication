package net.uweeisele.examples.kafka.avro.serializers.payload;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

public class AvroBytesPayload implements AvroBytes {

    protected final Payload payload;

    public AvroBytesPayload(ByteBuffer avroBytes) {
        this();
        withAvroBytes(avroBytes);
    }

    public AvroBytesPayload(byte[] avroBytes) {
        this();
        withAvroBytes(avroBytes);
    }

    public AvroBytesPayload() {
        this(new Payload());
    }

    public AvroBytesPayload(Payload payload) {
        this.payload = requireNonNull(payload);
    }

    public AvroBytesPayload withAvroBytes(byte[] avroBytes) {
        return withAvroBytes(avroBytes != null ? ByteBuffer.wrap(avroBytes) : null);
    }

    @Override
    public AvroBytesPayload withAvroBytes(ByteBuffer avroBytes) {
        payload.put(AvroBytes.KEY, avroBytes);
        return this;
    }

    @Override
    public ByteBuffer avroBytes() {
        return payload.get(AvroBytes.KEY);
    }

    @Override
    public Payload payload() {
        return payload;
    }

}
