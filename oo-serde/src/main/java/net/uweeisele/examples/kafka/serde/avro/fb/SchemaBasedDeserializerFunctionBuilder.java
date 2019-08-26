package net.uweeisele.examples.kafka.serde.avro.fb;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class SchemaBasedDeserializerFunctionBuilder<T, S> implements FunctionBuilder<Payload<byte[]>, Payload<T>, Config> {

    private FunctionBuilder<Payload<byte[]>, Payload<? extends DeserializableSchemaBasedData<S>>, Config> writerSchemaDeserializerBuilder;
    private FunctionBuilder<Payload<? extends DeserializableSchemaBasedData<S>>, Payload<T>, Config> contentDeserializerBuilder;

    @Override
    public Function<Payload<byte[]>, Payload<T>> build(Config config) {
        return writerSchemaDeserializerBuilder
                .andThen(contentDeserializerBuilder)
                .build(config);
    }

    public SchemaBasedDeserializerFunctionBuilder<T, S> withWriterSchemaDeserializer(Function<Payload<byte[]>, Payload<? extends DeserializableSchemaBasedData<S>>> writerSchemaDeserializer) {
        requireNonNull(writerSchemaDeserializer);
        return withWriterSchemaDeserializerBuilder(c -> writerSchemaDeserializer);
    }

    public SchemaBasedDeserializerFunctionBuilder<T, S> withWriterSchemaDeserializerBuilder(FunctionBuilder<Payload<byte[]>, Payload<? extends DeserializableSchemaBasedData<S>>, Config> writerSchemaDeserializerBuilder) {
        this.writerSchemaDeserializerBuilder = requireNonNull(writerSchemaDeserializerBuilder);
        return this;
    }

    public SchemaBasedDeserializerFunctionBuilder<T, S> withContentDeserializer(Function<Payload<? extends DeserializableSchemaBasedData<S>>, Payload<T>> contentDeserializer) {
        requireNonNull(contentDeserializer);
        return withContentDeserializerBuilder(c -> contentDeserializer);
    }

    public SchemaBasedDeserializerFunctionBuilder<T, S> withContentDeserializerBuilder(FunctionBuilder<Payload<? extends DeserializableSchemaBasedData<S>>, Payload<T>, Config> contentDeserializerBuilder) {
        this.contentDeserializerBuilder = requireNonNull(contentDeserializerBuilder);
        return this;
    }

}
