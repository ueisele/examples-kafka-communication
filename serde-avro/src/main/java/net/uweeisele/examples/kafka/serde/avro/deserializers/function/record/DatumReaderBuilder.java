package net.uweeisele.examples.kafka.serde.avro.deserializers.function.record;

import java.util.function.Function;

public interface DatumReaderBuilder<S, D> extends Function<S, SchemaAwareDatumReader<D>> {

    @Override
    default SchemaAwareDatumReader<D> apply(S payload) {
        return build(payload);
    }

    SchemaAwareDatumReader<D> build(S payload);

}
