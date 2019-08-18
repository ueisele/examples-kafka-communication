package net.uweeisele.examples.kafka.serde.avro.deserializers.function.schema;

import net.uweeisele.examples.kafka.serde.avro.deserializers.payload.DataAware;
import org.apache.kafka.common.errors.SerializationException;

import java.nio.ByteBuffer;
import java.util.function.Function;

public class DataSchemaIdDeserializer<S extends DataAware> implements Function<S, Integer> {

    @Override
    public Integer apply(S payload) throws SerializationException {
        ByteBuffer data = payload.data();
        if (data.get() != 0) {
            // throw Invalid Format Exception (No Avro) -> Not Retryable
            throw new SerializationException("Unknown magic byte!");
        }
        return data.getInt();
    }

}
