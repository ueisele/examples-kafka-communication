package net.uweeisele.examples.kafka.serde.avro.protocol.matcher;

import java.util.Properties;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

public interface SchemaMatcherBuilder extends Function<Properties, SchemaMatcher>, Supplier<SchemaMatcher>, Predicate<Properties> {

    @Override
    default SchemaMatcher get() {
        return build();
    }

    @Override
    default SchemaMatcher apply(Properties properties) {
        return build(properties);
    }

    default SchemaMatcher build() {
        return build(new Properties());
    }

    SchemaMatcher build(Properties properties);

}
