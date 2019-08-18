package net.uweeisele.examples.kafka.serde.avro.deserializers.function.record;

import net.uweeisele.examples.kafka.serde.avro.deserializers.payload.DataAware;
import net.uweeisele.examples.kafka.serde.avro.deserializers.payload.ReaderSchemaAware;
import net.uweeisele.examples.kafka.serde.avro.deserializers.payload.WriterSchemaAware;
import net.uweeisele.examples.kafka.serde.avro.function.ConfigurableFunction;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.ByteBufferInputStream;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.io.InputStream;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public class AvroRecordDeserializer<S extends DataAware & WriterSchemaAware & ReaderSchemaAware, T extends IndexedRecord> implements ConfigurableFunction<S, T> {

    private static final String KEY_DATUM_READER = "avro.datum.reader";

    private final DatumReaderBuilder<S, T> datumReaderBuilder;

    private final DecoderFactory decoderFactory;

    public AvroRecordDeserializer(DatumReaderBuilder<S, T> datumReaderBuilder) {
        this(datumReaderBuilder, DecoderFactory.get());
    }

    protected AvroRecordDeserializer(DatumReaderBuilder<S, T> datumReaderBuilder, DecoderFactory decoderFactory) {
        this.datumReaderBuilder = requireNonNull(datumReaderBuilder);
        this.decoderFactory = requireNonNull(decoderFactory);
    }

    @Override
    public T apply(S payload) throws SerializationException {
        DatumReader<T> datumReader = datumReaderBuilder.build(payload);
        payload.withAttribute(KEY_DATUM_READER, String.valueOf(datumReader));
        return extract(payload, datumReader);
    }

    private T extract(S payload, DatumReader<T> reader) throws SerializationException {
        try(InputStream in = new ByteBufferInputStream(singletonList(payload.data().duplicate()))) {
            return reader.read(null, decoderFactory.binaryDecoder(in, null));
        } catch (IOException | RuntimeException e) {
            // avro deserialization may throw AvroRuntimeException, NullPointerException, etc
            throw new SerializationException("Error deserializing Avro message for id " + "todo", e);
        }
    }

}
