package net.uweeisele.examples.kafka.serde.avro.protocol.avro.schema;

import org.apache.avro.Schema;

public interface AvroSchemaRegistryClient {

    int register(String subject, Schema schema);

    int getId(String subject, Schema schema);

    Schema getById(int id);

}
