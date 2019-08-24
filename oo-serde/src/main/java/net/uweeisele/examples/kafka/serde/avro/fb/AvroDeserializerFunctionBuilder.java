package net.uweeisele.examples.kafka.serde.avro.fb;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class AvroDeserializerFunctionBuilder<T> implements FunctionBuilder<Payload<byte[]>, Payload<T>, Config> {

    private FunctionBuilder<Payload<byte[]>, Payload<? extends AvroData>, Config> writerSchemaDeserializerBuilder;
    private FunctionBuilder<Payload<? extends AvroData>, Payload<T>, Config> avroDeserializerBuilder;

    @Override
    public Function<Payload<byte[]>, Payload<T>> build(Config config) {
        return writerSchemaDeserializerBuilder
                .andThen(avroDeserializerBuilder)
                .build(config);
    }

    public AvroDeserializerFunctionBuilder<T> withWriterSchemaDeserializer(Function<Payload<byte[]>, Payload<? extends AvroData>> writerSchemaDeserializer) {
        requireNonNull(writerSchemaDeserializer);
        return withWriterSchemaDeserializerBuilder(c -> writerSchemaDeserializer);
    }

    public AvroDeserializerFunctionBuilder<T> withWriterSchemaDeserializerBuilder(FunctionBuilder<Payload<byte[]>, Payload<? extends AvroData>, Config> writerSchemaDeserializerBuilder) {
        this.writerSchemaDeserializerBuilder = requireNonNull(writerSchemaDeserializerBuilder);
        return this;
    }

    public AvroDeserializerFunctionBuilder<T> withAvroDeserializer(Function<Payload<? extends AvroData>, Payload<T>> avroDeserializer) {
        requireNonNull(avroDeserializer);
        return withAvroDeserializerBuilder(c -> avroDeserializer);
    }

    public AvroDeserializerFunctionBuilder<T> withAvroDeserializerBuilder(FunctionBuilder<Payload<? extends AvroData>, Payload<T>, Config> avroDeserializerBuilder) {
        this.avroDeserializerBuilder = requireNonNull(avroDeserializerBuilder);
        return this;
    }

}
