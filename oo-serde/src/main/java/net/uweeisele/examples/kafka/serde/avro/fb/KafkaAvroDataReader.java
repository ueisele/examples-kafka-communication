package net.uweeisele.examples.kafka.serde.avro.fb;

import org.apache.kafka.common.errors.SerializationException;

import java.nio.ByteBuffer;
import java.util.function.Function;

public class KafkaAvroDataReader implements Function<Payload<byte[]>, Payload<? extends ExternalizedSchemaBasedData<Integer>>> {

    private final Config config;

    public KafkaAvroDataReader(Config config) {
        this.config = config;
    }

    @Override
    public Integer apply(S payload) throws SerializationException {
        ByteBuffer data = payload.data();
        if (data.get() != 0) {
            // throw Invalid Format Exception (No Avro) -> Not Retryable
            throw new SerializationException("Unknown magic byte!");
        }
        return data.getInt();
    }

    @Override
    public Payload<? extends ExternalizedSchemaBasedData<Integer>> apply(Payload<byte[]> payload) {
        return null;
    }
}
