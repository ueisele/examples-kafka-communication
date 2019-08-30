package net.uweeisele.examples.kafka.serde.avro.protocol;

import java.io.IOException;
import java.util.Objects;

@FunctionalInterface
public interface ProtocolFilter<T> extends ProtocolDecoder<T, T>, ProtocolEncoder<T, T> {

    @Override
    default Payload<T> decode(Payload<T> input) throws IOException {
        return filter(input);
    }

    @Override
    default Payload<T> encode(Payload<T> input) throws IOException {
        return filter(input);
    }

    Payload<T> filter(Payload<T> input) throws IOException;

    default ProtocolFilter<T> compose(ProtocolFilter<T> before) {
        Objects.requireNonNull(before);
        return (Payload<T> input) -> filter(before.filter(input));
    }

    default ProtocolFilter<T> andThen(ProtocolFilter<T> after) {
        Objects.requireNonNull(after);
        return (Payload<T> input) -> after.filter(filter(input));
    }

    static <T> ProtocolFilter<T> identity() {
        return t -> t;
    }

}
