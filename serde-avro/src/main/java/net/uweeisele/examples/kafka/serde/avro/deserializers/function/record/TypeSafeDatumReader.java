package net.uweeisele.examples.kafka.serde.avro.deserializers.function.record;

import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TypeSafeDatumReader<D> implements SchemaAwareDatumReader<D> {

    private final Class<? extends D> type;

    private final SchemaAwareDatumReader<D> datumReader;

    public TypeSafeDatumReader(Class<? extends D> type, SchemaAwareDatumReader<D> datumReader) {
        this.type = requireNonNull(type);
        this.datumReader = requireNonNull(datumReader);
    }

    @Override
    public D read(D reuse, Decoder in) throws IOException {
        D value = datumReader.read(reuse, in);
        if (value != null && !type.isAssignableFrom(value.getClass())) {
            throw new SerializationException(format("Deserialized Avro datum is a %s, but expected a %s.", value.getClass().getName(), type.getName()));
        }
        return value;
    }

    @Override
    public Schema getSchema() {
        return datumReader.getSchema();
    }

    @Override
    public void setSchema(Schema schema) {
        datumReader.setSchema(schema);
    }

    @Override
    public Schema getExpected() {
        return datumReader.getExpected();
    }

    @Override
    public void setExpected(Schema reader) {
            datumReader.setExpected(reader);
    }

    @Override
    public String toString() {
        return "TypeSafeDatumReader{" +
                "datumReader=" + datumReader +
                ", type=" + type +
                '}';
    }
}
