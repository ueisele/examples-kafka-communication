package net.uweeisele.examples.kafka.avro.serializers.extractor;

import net.uweeisele.examples.kafka.avro.serializers.function.ConfigurableFunction;
import org.apache.kafka.common.errors.SerializationException;

public interface PayloadExtractor<S, D> extends ConfigurableFunction<S, D> {

    @Override
    default D apply(S payload) throws SerializationException {
        return extract(payload);
    }

    D extract(S payload) throws SerializationException;

}
