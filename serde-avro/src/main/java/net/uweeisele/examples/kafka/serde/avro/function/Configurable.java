package net.uweeisele.examples.kafka.serde.avro.function;

import java.util.Properties;

public interface Configurable {

    default Configurable configure(Properties properties) {
        return this;
    }

}
