package net.uweeisele.examples.kafka.serde.avro.deserializers.payload;

import org.apache.avro.Schema;

import java.util.function.Supplier;

public interface WriterSchemaAware extends PayloadAware {

    Schema writerSchema();

    WriterSchemaAware withWriterSchema(Schema writerSchema);

    Schema computeWriterSchemaIfAbsent(Supplier<Schema> supplier);
}
