package net.uweeisele.examples.kafka.serde.avro.protocol;

public interface Protocol<S, C> {

    S schema();

    C content();
}
