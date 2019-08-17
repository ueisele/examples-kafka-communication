package net.uweeisele.examples.kafka.avro.serializers.function;

import java.util.Properties;

public interface Configurable {

    default void configure(Properties properties) { }
}
