package net.uweeisele.examples.kafka.serde.avro.deserializers;

import net.uweeisele.examples.kafka.serde.avro.builder.PropertiesBuilder;
import net.uweeisele.examples.kafka.serde.avro.deserializers.payload.*;
import net.uweeisele.examples.kafka.serde.avro.function.Configurable;
import net.uweeisele.examples.kafka.serde.avro.function.ConfigurableFunction;
import org.apache.avro.Schema;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class BaseKafkaAvroDeserializer<S extends TopicAware & HeadersAware & DataAware & ReaderSchemaAware, T> implements Deserializer<T> {

    private final Supplier<? extends S> payloadSupplier;
    private final Function<? super S, T> deserializer;

    private Properties configs;
    private boolean isKey;

    public <R extends DataAware & WriterSchemaAware & ReaderSchemaAware> BaseKafkaAvroDeserializer(Supplier<? extends S> payloadSupplier, Function<? super S, ? extends R> schemaDeserializer, Function<? super R, ? extends T> recordDeserializer) {
        this(payloadSupplier,
                ConfigurableFunction.<S>identity()
                        .andThen(schemaDeserializer)
                        .andThen(recordDeserializer)
        );
    }

    public BaseKafkaAvroDeserializer(Supplier<? extends S> payloadSupplier, Function<? super S, T> deserializer) {
        this.payloadSupplier = requireNonNull(payloadSupplier);
        this.deserializer = requireNonNull(deserializer);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        configure(PropertiesBuilder.ofNormalized(configs), isKey);
    }

    public void configure(Properties properties, boolean isKey) {
        this.configs = requireNonNull(properties);
        this.isKey = isKey;
        if (this.deserializer instanceof Configurable) {
            ((Configurable) this.deserializer).configure(properties);
        }
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        return deserialize(topic, new RecordHeaders(), data);
    }

    @Override
    public T deserialize(String topic, Headers headers, byte[] data) {
        return deserialize(topic, headers, data, null);
    }

    public T deserialize(String topic, Headers headers, byte[] data, Schema readerSchema) {
        if (data == null) {
            return null;
        }

        S payload = payloadSupplier.get();
        payload.withTopic(topic);
        payload.withIsKey(isKey);
        payload.withHeaders(headers);
        payload.withData(data);
        payload.withReaderSchema(readerSchema);
        payload.setConfigs(configs);

        return deserializer.apply(payload);
    }
}
