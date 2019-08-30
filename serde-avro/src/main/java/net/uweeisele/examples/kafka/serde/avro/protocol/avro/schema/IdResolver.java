package net.uweeisele.examples.kafka.serde.avro.protocol.avro.schema;

import org.apache.avro.Schema;

@FunctionalInterface
public interface IdResolver {

    int getIdBySubjectAndSchema(String subject, Schema schema);
}
