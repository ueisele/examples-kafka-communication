package net.uweeisele.examples.kafka.avro.serializers.enricher;

import net.uweeisele.examples.kafka.avro.serializers.function.ConfigurableBiFunction;
import org.apache.kafka.common.errors.SerializationException;

public interface PayloadEnricher<S, E, D> extends ConfigurableBiFunction<S, E, D> {

    @Override
    default D apply(S payload, E element) throws SerializationException {
        return enrich(payload, element);
    }

    D enrich(S payload, E element) throws SerializationException;
}
