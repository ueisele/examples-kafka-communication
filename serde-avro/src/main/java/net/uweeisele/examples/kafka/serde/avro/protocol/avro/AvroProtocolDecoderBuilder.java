package net.uweeisele.examples.kafka.serde.avro.protocol.avro;

import net.uweeisele.examples.kafka.serde.avro.protocol.ProtocolDecoder;

import java.util.Properties;
import java.util.function.Function;

public class AvroProtocolDecoderBuilder<T> implements Function<Properties, ProtocolDecoder<byte[], T>> {

    @Override
    public ProtocolDecoder<byte[], T> apply(Properties properties) {
        return null;
    }
}
