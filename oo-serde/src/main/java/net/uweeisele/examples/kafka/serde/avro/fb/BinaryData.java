package net.uweeisele.examples.kafka.serde.avro.fb;

import java.nio.ByteBuffer;

public interface BinaryData {

    ByteBuffer contentBytes();
}
