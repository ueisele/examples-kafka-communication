package net.uweeisele.examples.kafka.avro.serializers.payload;

import net.uweeisele.examples.kafka.avro.serializers.payload.Payload.Key;
import org.apache.avro.Schema;

public interface WriterSchema extends PayloadContainer {

    Key<Schema> KEY = new Key<>(WriterSchema.class.getSimpleName());

    Schema writerSchema();

    WriterSchema withWriterSchema(Schema writerSchema);
}
