package net.uweeisele.examples.kafka.serde.avro.protocol;

public class ProtocolContainer<S, C> implements Protocol<S, C> {

    private final S schema;

    private final C content;

    public ProtocolContainer(S schema, C content) {
        this.schema = schema;
        this.content = content;
    }

    @Override
    public S schema() {
        return schema;
    }

    @Override
    public C content() {
        return content;
    }
}
