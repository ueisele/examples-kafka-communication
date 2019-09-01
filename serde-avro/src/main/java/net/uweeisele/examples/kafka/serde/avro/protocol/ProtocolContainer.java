package net.uweeisele.examples.kafka.serde.avro.protocol;

import java.util.Collections;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class ProtocolContainer<S, C> implements Protocol<S, C>, ContextSupplier {

    private final S schema;

    private final C content;

    private final ContextSupplier contextSupplier;

    public ProtocolContainer(S schema, C content) {
        this(schema, content, Collections::emptyMap);
    }

    public ProtocolContainer(S schema, C content, ContextSupplier contextSupplier) {
        this.schema = schema;
        this.content = content;
        this.contextSupplier = requireNonNull(contextSupplier);
    }

    @Override
    public S schema() {
        return schema;
    }

    @Override
    public C content() {
        return content;
    }

    @Override
    public Map<String, String> context() {
        return contextSupplier.context();
    }
}
