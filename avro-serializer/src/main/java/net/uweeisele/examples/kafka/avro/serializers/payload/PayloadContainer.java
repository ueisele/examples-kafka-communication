package net.uweeisele.examples.kafka.avro.serializers.payload;

import java.util.function.Consumer;

public interface PayloadContainer {

    Payload payload();

    default PayloadContainer with(Consumer<Payload> action) {
        action.accept(payload());
        return this;
    }
}
