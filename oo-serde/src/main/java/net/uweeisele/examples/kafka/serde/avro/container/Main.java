package net.uweeisele.examples.kafka.serde.avro.container;

public class Main {

    public static void main(String[] args) {
        DataWriterSchemaContainer<DataSchemaIdContainer<DeserializerContext, DeserializerContext>, DeserializerContext> d = new DataWriterSchemaContainer<>(new DataSchemaIdContainer<>(new DeserializerContext()));
    }
}
