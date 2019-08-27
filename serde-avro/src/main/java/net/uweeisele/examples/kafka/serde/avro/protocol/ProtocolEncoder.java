package net.uweeisele.examples.kafka.serde.avro.protocol;

import java.io.IOException;
import java.util.Objects;

@FunctionalInterface
public interface ProtocolEncoder<I, O> {

    Payload<O> encode(Payload<I> input) throws IOException;

    default <V> ProtocolEncoder<V, O> compose(ProtocolEncoder<V, I> before) {
        Objects.requireNonNull(before);
        return (Payload<V> input) -> encode(before.encode(input));
    }

    default <V> ProtocolEncoder<I, V> andThen(ProtocolEncoder<O, V> after) {
        Objects.requireNonNull(after);
        return (Payload<I> input) -> after.encode(encode(input));
    }

    static <T> ProtocolEncoder<T, T> identity() {
        return t -> t;
    }
}
