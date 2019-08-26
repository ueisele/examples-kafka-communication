package net.uweeisele.examples.kafka.serde.avro.fb;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class ExternalizedSchemaDeserializerFunctionBuilder<I, S> implements FunctionBuilder<Payload<byte[]>, Payload<? extends DeserializableSchemaBasedData<S>>, Config> {

    private FunctionBuilder<Payload<byte[]>, Payload<? extends ExternalizedSchemaBasedData<I>>, Config> schemaIdDeserializerBuilder;
    private FunctionBuilder<Payload<? extends ExternalizedSchemaBasedData<I>>, Payload<? extends DeserializableSchemaBasedData<S>>, Config> schemaResolverBuilder;

    @Override
    public Function<Payload<byte[]>, Payload<? extends DeserializableSchemaBasedData<S>>> build(Config config) {
        return schemaIdDeserializerBuilder
                .andThen(schemaResolverBuilder)
                .build(config);
    }

    public ExternalizedSchemaDeserializerFunctionBuilder<I, S> withSchemaIdDeserializer(Function<Payload<byte[]>, Payload<? extends ExternalizedSchemaBasedData<I>>> schemaIdDeserializer) {
        requireNonNull(schemaIdDeserializer);
        return withSchemaIdDeserializerBuilder(c -> schemaIdDeserializer);
    }

    public ExternalizedSchemaDeserializerFunctionBuilder<I, S> withSchemaIdDeserializerBuilder(FunctionBuilder<Payload<byte[]>, Payload<? extends ExternalizedSchemaBasedData<I>>, Config> schemaIdDeserializerBuilder) {
        this.schemaIdDeserializerBuilder = requireNonNull(schemaIdDeserializerBuilder);
        return this;
    }

    public ExternalizedSchemaDeserializerFunctionBuilder<I, S>withSchemaResolver(Function<Payload<? extends ExternalizedSchemaBasedData<I>>, Payload<? extends DeserializableSchemaBasedData<S>>> schemaResolver) {
        requireNonNull(schemaResolver);
        return withSchemaResolverBuilder(c -> schemaResolver);
    }

    public ExternalizedSchemaDeserializerFunctionBuilder<I, S> withSchemaResolverBuilder(FunctionBuilder<Payload<? extends ExternalizedSchemaBasedData<I>>, Payload<? extends DeserializableSchemaBasedData<S>>, Config> schemaResolverBuilder) {
        this.schemaResolverBuilder = requireNonNull(schemaResolverBuilder);
        return this;
    }

}
