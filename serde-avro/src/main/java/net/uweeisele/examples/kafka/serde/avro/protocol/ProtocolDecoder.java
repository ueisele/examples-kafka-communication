package net.uweeisele.examples.kafka.serde.avro.protocol;

import java.io.IOException;
import java.util.Objects;

@FunctionalInterface
public interface ProtocolDecoder<I, O> {

    Payload<O> decode(Payload<I> input) throws IOException;

    default <V> ProtocolDecoder<V, O> compose(ProtocolDecoder<V, I> before) {
        Objects.requireNonNull(before);
        return (Payload<V> input) -> decode(before.decode(input));
    }

    default <V> ProtocolDecoder<I, V> andThen(ProtocolDecoder<O, V> after) {
        Objects.requireNonNull(after);
        return (Payload<I> input) -> after.decode(decode(input));
    }

    static <T> ProtocolDecoder<T, T> identity() {
        return t -> t;
    }

}
