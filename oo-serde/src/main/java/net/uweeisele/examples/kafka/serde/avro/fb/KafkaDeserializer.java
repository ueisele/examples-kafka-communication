package net.uweeisele.examples.kafka.serde.avro.fb;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static net.uweeisele.examples.kafka.serde.avro.builder.PropertiesBuilder.ofNormalized;

public class KafkaDeserializer<T> implements Deserializer<T> {

    private final FunctionBuilder<Payload<byte[]>, Payload<T>, Config> deserializerBuilder;

    private Function<Payload<byte[]>, Payload<T>> deserializer;

    public KafkaDeserializer(FunctionBuilder<Payload<byte[]>, Payload<T>, Config> deserializerBuilder) {
        this.deserializerBuilder = requireNonNull(deserializerBuilder);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        configure(ofNormalized(configs), isKey);
    }

    public Deserializer<T> configure(Properties properties, boolean isKey) {
        return configure(new Config(properties, isKey));
    }

    public Deserializer<T> configure(Config config) {
        this.deserializer = deserializerBuilder.build(config);
        return this;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        return deserialize(topic, new RecordHeaders(), data);
    }

    @Override
    public T deserialize(String topic, Headers headers, byte[] data) {
        if (data == null) {
            return null;
        }

        Payload<byte[]> payload = new Payload<>(topic, headers, data);

        return deserializer.apply(payload).get();
    }

}
