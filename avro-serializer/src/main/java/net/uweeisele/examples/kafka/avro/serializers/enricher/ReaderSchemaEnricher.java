package net.uweeisele.examples.kafka.avro.serializers.enricher;

import net.uweeisele.examples.kafka.avro.serializers.payload.ReaderSchema;
import org.apache.avro.Schema;
import org.apache.kafka.common.errors.SerializationException;

import java.util.function.Function;

public class ReaderSchemaEnricher<S, D extends ReaderSchema> implements PayloadEnricher<S, Schema, D> {

    private final Function<S, D> payloadBuilder;

    public ReaderSchemaEnricher(Function<S, D> payloadBuilder) {
        this.payloadBuilder = payloadBuilder;
    }

    @Override
    public D enrich(S payload, Schema element) throws SerializationException {
        D enrichedPayload = payloadBuilder.apply(payload);
        enrichedPayload.withReaderSchema(element);
        return enrichedPayload;
    }
}
