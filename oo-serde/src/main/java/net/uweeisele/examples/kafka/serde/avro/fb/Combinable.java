package net.uweeisele.examples.kafka.serde.avro.fb;

public interface Combinable<T> {

    T combine(T t);

}
