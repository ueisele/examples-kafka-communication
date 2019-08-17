package net.uweeisele.examples.kafka.avro.serializers.payload;

import net.uweeisele.examples.kafka.avro.serializers.payload.Payload.Key;
import org.apache.avro.Schema;

public interface ReaderSchema extends PayloadContainer {

    Key<Schema> KEY = new Key<>(ReaderSchema.class.getSimpleName());

    Schema readerSchema();

    ReaderSchema withReaderSchema(Schema readerSchema);
}
