package net.uweeisele.examples.kafka.serde.avro.deserializers.payload;

import java.nio.ByteBuffer;

public interface DataAware extends PayloadAware {

    ByteBuffer data();

    DataAware withData(byte[] data);

    DataAware withData(ByteBuffer data);
}
