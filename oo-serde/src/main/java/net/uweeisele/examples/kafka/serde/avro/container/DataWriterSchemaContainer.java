package net.uweeisele.examples.kafka.serde.avro.container;

import net.uweeisele.examples.kafka.serde.avro.container.aware.DataAware;
import net.uweeisele.examples.kafka.serde.avro.container.aware.TopicAware;
import net.uweeisele.examples.kafka.serde.avro.container.aware.WriterSchemaAware;
import net.uweeisele.examples.kafka.serde.avro.container.aware.WriterSchemaIdAware;
import org.apache.avro.Schema;

public class DataWriterSchemaContainer<P extends Container<C> & WriterSchemaIdAware, C extends Container<C> & TopicAware & DataAware> implements Container<C>, WriterSchemaAware {

    private final P payload;
    private final C context;

    public DataWriterSchemaContainer(P payload) {
        this.payload = payload;
        this.context = payload.context();
    }

    @Override
    public C context() {
        return context;
    }

    @Override
    public Schema writerSchema() {
        return null;
    }
}
