package net.uweeisele.examples.kafka.avro.serializers.extractor.deserializer;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TypeSafeDatumReader<D> implements DatumReader<D> {

    private final Class<D> type;

    private final DatumReader<D> datumReader;

    public TypeSafeDatumReader(Class<D> type, DatumReader<D> datumReader) {
        this.type = requireNonNull(type);
        this.datumReader = requireNonNull(datumReader);
    }

    @Override
    public void setSchema(Schema schema) {
        datumReader.setSchema(schema);
    }

    @Override
    public D read(D reuse, Decoder in) throws IOException {
        D value = datumReader.read(reuse, in);
        if (value != null && !type.isAssignableFrom(value.getClass())) {
            throw new SerializationException(format("Deserialized Avro datum is a %s, but expected a %s.", value.getClass().getName(), type.getName()));
        }
        return value;
    }

}
