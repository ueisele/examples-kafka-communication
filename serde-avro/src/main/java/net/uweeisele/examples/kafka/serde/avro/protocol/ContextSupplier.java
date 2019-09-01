package net.uweeisele.examples.kafka.serde.avro.protocol;

import java.util.Map;

@FunctionalInterface
public interface ContextSupplier {

    Map<String, String> context();
}
