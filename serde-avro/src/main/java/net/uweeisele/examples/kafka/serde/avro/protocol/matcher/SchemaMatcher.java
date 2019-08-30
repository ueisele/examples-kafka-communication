package net.uweeisele.examples.kafka.serde.avro.protocol.matcher;

public interface SchemaMatcher {

    SchemaClassification matches(String name);

    enum SchemaClassification {
        UNKNOWN,
        KNOWN,
        ACCEPTED
    }
}
