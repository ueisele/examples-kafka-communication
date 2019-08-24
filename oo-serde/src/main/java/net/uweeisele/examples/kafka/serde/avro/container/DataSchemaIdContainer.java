package net.uweeisele.examples.kafka.serde.avro.container;

import net.uweeisele.examples.kafka.serde.avro.container.aware.DataAware;
import net.uweeisele.examples.kafka.serde.avro.container.aware.WriterSchemaIdAware;

public class DataSchemaIdContainer<P extends Container<C>, C extends Container<?> & DataAware> implements Container<C>, WriterSchemaIdAware {

    private final C context;

    public DataSchemaIdContainer(P payload) {
        this.context = payload.context();
    }

    @Override
    public C context() {
        return context;
    }

    @Override
    public int writerSchemaId() {
        return 0;
    }
}
