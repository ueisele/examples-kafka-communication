package net.uweeisele.examples.kafka.serde.avro.container.aware;

import org.apache.avro.Schema;

import java.util.function.Supplier;

public interface ReaderSchemaAware extends PayloadAware {

    Schema readerSchema();

    ReaderSchemaAware withReaderSchema(Schema readerSchema);

    Schema computeReaderSchemaIfAbsent(Supplier<Schema> supplier);
}
