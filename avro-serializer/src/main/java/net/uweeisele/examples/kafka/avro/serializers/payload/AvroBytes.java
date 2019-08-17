package net.uweeisele.examples.kafka.avro.serializers.payload;

import net.uweeisele.examples.kafka.avro.serializers.payload.Payload.Key;

import java.nio.ByteBuffer;

public interface AvroBytes extends PayloadContainer {

    Key<ByteBuffer> KEY = new Key<>(AvroBytes.class.getSimpleName());

    ByteBuffer avroBytes();

    AvroBytes withAvroBytes(ByteBuffer avroBytes);
}
