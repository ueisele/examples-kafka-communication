package net.uweeisele.examples.kafka.serde.avro.protocol.avro.schema;

import org.apache.avro.Schema;

@FunctionalInterface
public interface SchemaResolver {

    Schema getSchemaById(int id);
}
