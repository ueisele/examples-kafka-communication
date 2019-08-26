package net.uweeisele.examples.kafka.serde.avro.fb;

import java.nio.ByteBuffer;

public interface ExternalizedSchemaBasedData<I> {

    I schemaId();

    ByteBuffer contentBytes();
}
