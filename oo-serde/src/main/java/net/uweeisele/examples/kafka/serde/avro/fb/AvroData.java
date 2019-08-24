package net.uweeisele.examples.kafka.serde.avro.fb;

import org.apache.avro.Schema;

import java.nio.ByteBuffer;

public interface AvroData {

    Schema schema();

    ByteBuffer avroBytes();
}
