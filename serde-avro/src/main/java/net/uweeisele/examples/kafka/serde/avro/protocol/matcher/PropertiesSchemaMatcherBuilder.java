package net.uweeisele.examples.kafka.serde.avro.protocol.matcher;

import java.util.List;
import java.util.Properties;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static net.uweeisele.examples.kafka.serde.avro.protocol.matcher.SchemaMatcher.SchemaClassification.ACCEPTED;

public class PropertiesSchemaMatcherBuilder implements SchemaMatcherBuilder {

    private final List<SchemaMatcherBuilder> builders;

    public PropertiesSchemaMatcherBuilder() {
        this(asList(new TriPatternSchemaMatcherBuilder(), new PredicateSchemaMatcherBuilder()));
    }

    public PropertiesSchemaMatcherBuilder(List<SchemaMatcherBuilder> builders) {
        this.builders = requireNonNull(builders);
    }

    @Override
    public boolean test(Properties properties) {
        return true;
    }

    @Override
    public SchemaMatcher build(Properties properties) {
        for (SchemaMatcherBuilder builder : builders) {
            if (builder.test(properties)) {
                return builder.build(properties);
            }
        }
        return name -> ACCEPTED;
    }

}
