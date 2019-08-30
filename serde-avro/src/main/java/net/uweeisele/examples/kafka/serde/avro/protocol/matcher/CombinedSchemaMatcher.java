package net.uweeisele.examples.kafka.serde.avro.protocol.matcher;

import java.util.List;

import static net.uweeisele.examples.kafka.serde.avro.protocol.matcher.SchemaMatcher.SchemaClassification.ACCEPTED;
import static net.uweeisele.examples.kafka.serde.avro.protocol.matcher.SchemaMatcher.SchemaClassification.UNKNOWN;

public class CombinedSchemaMatcher implements SchemaMatcher {

    private final List<SchemaMatcher> schemaMatchers;

    public CombinedSchemaMatcher(List<SchemaMatcher> schemaMatchers) {
        this.schemaMatchers = schemaMatchers;
    }

    @Override
    public SchemaClassification matches(String name) {
        SchemaClassification classification = UNKNOWN;
        for (SchemaMatcher matcher : schemaMatchers) {
            SchemaClassification current = matcher.matches(name);
            if (current.compareTo(classification) > 0) {
                classification = current;
            }
            if (classification.equals(ACCEPTED)) {
                break;
            }
        }
        return classification;
    }
}
