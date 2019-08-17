package net.uweeisele.examples.kafka.avro.serializers.payload;

import net.uweeisele.examples.kafka.avro.serializers.payload.Payload.Key;

public interface WriterSchemaId extends PayloadContainer {

    Key<Integer> KEY = new Key<>(WriterSchemaId.class.getSimpleName());

    Integer writerSchemaId();

    WriterSchemaId withWriterSchemaId(Integer writerSchemaId);
}
