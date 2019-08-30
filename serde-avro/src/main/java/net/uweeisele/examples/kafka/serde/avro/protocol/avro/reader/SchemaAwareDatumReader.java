package net.uweeisele.examples.kafka.serde.avro.protocol.avro.reader;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;

public interface SchemaAwareDatumReader<D> extends DatumReader<D> {

    Schema getSchema();

    Schema getExpected();

    void setExpected(Schema reader);

}
