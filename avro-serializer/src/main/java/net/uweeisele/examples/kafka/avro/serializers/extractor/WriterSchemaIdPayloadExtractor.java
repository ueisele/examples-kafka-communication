package net.uweeisele.examples.kafka.avro.serializers.extractor;

import net.uweeisele.examples.kafka.avro.serializers.payload.AvroBytes;
import net.uweeisele.examples.kafka.avro.serializers.payload.AvroBytesWriterSchemaIdPayload;
import org.apache.kafka.common.errors.SerializationException;

public class WriterSchemaIdPayloadExtractor<S extends AvroBytes> implements PayloadExtractor<S, AvroBytesWriterSchemaIdPayload> {

    @Override
    public AvroBytesWriterSchemaIdPayload extract(S payload) throws SerializationException {
        if (payload.avroBytes().get() != 0) {
            // throw Invalid Format Exception (No Avro) -> Not Retryable
            throw new SerializationException("Unknown magic byte!");
        } else {
            int schemaId = payload.avroBytes().getInt();
            return new AvroBytesWriterSchemaIdPayload(payload)
                    .withWriterSchemaId(schemaId);
        }
    }
}
