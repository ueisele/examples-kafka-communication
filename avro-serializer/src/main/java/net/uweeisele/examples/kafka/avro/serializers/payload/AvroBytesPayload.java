package net.uweeisele.examples.kafka.avro.serializers.payload;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.function.Function;

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

    public static Function<byte[], AvroBytesPayload> bytesToAvroBytesFunction() {
        return AvroBytesPayload::new;
    }

    public static Function<ByteBuffer, AvroBytesPayload> byteBufferToAvroBytesFunction() {
        return AvroBytesPayload::new;
    }

    public AvroBytesPayload withAvroBytes(byte[] avroBytes) {
        return withAvroBytes(ByteBuffer.wrap(avroBytes));
    }

    @Override
    public AvroBytesPayload withAvroBytes(ByteBuffer avroBytes) {
        payload.put(AvroBytes.KEY, avroBytes);
        return this;
    }

    @Override
    public AvroBytesPayload with(Consumer<Payload> action) {
        action.accept(payload());
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
