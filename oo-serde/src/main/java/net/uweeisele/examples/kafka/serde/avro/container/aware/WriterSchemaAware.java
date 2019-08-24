package net.uweeisele.examples.kafka.serde.avro.container.aware;

import org.apache.avro.Schema;

public interface WriterSchemaAware {

    Schema writerSchema();

}
