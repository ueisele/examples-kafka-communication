package net.uweeisele.examples.kafka.avro.serializers.extractor;

import net.uweeisele.examples.kafka.avro.serializers.payload.AvroBytes;
import net.uweeisele.examples.kafka.avro.serializers.payload.AvroReader;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.ByteBufferInputStream;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public class AvroRecordPayloadExtractor<S extends AvroBytes & AvroReader<T>, T extends IndexedRecord> implements PayloadExtractor<S, T> {

    private final DecoderFactory decoderFactory;

    public AvroRecordPayloadExtractor() {
        this(DecoderFactory.get());
    }

    protected AvroRecordPayloadExtractor(DecoderFactory decoderFactory) {
        this.decoderFactory = requireNonNull(decoderFactory);
    }

    @Override
    public T extract(S payload) throws SerializationException {
        return extract(payload.avroBytes(), payload.avroReader());
    }

    private T extract(ByteBuffer avroBytes, DatumReader<T> reader) throws SerializationException {
        try(InputStream in = new ByteBufferInputStream(singletonList(avroBytes.duplicate()))) {
            return reader.read(null, decoderFactory.binaryDecoder(in, null));
        } catch (IOException | RuntimeException e) {
            // avro deserialization may throw AvroRuntimeException, NullPointerException, etc
            throw new SerializationException("Error deserializing Avro message for id " + "todo", e);
        }
    }

}
