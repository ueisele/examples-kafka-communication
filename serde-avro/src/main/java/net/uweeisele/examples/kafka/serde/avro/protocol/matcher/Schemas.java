package net.uweeisele.examples.kafka.serde.avro.protocol.matcher;

import static net.uweeisele.examples.kafka.serde.avro.protocol.matcher.SchemaMatcher.SchemaClassification.ACCEPTED;

public class Schemas {

    public static Schemas accept() {
       return new Schemas();
    }

    public SchemaMatcher all() {
        return name -> ACCEPTED;
    }

    public PredicateSchemaMatcherBuilder withPredicateFilter() {
        return new PredicateSchemaMatcherBuilder();
    }

    public TriPatternSchemaMatcherBuilder withTriPatternFilter() {
        return new TriPatternSchemaMatcherBuilder();
    }

}
