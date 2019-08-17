package net.uweeisele.examples.kafka.avro.serializers.extractor;

import net.uweeisele.examples.kafka.avro.serializers.extractor.deserializer.DatumReaderBuilder;
import net.uweeisele.examples.kafka.avro.serializers.payload.AvroBytes;
import net.uweeisele.examples.kafka.avro.serializers.payload.AvroBytesReaderPayload;
import net.uweeisele.examples.kafka.avro.serializers.payload.ReaderSchema;
import net.uweeisele.examples.kafka.avro.serializers.payload.WriterSchema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.errors.SerializationException;

import static java.util.Objects.requireNonNull;

public class DatumReaderPayloadExtractor<S extends AvroBytes & WriterSchema & ReaderSchema, T extends IndexedRecord> implements PayloadExtractor<S, AvroBytesReaderPayload<T>> {

    private final DatumReaderBuilder<T> datumReaderBuilder;

    public DatumReaderPayloadExtractor(DatumReaderBuilder<T> datumReaderBuilder) {
        this.datumReaderBuilder = requireNonNull(datumReaderBuilder);
    }

    @Override
    public AvroBytesReaderPayload<T> extract(S payload) throws SerializationException {
        return new AvroBytesReaderPayload<T>(payload)
                .withAvroReader(datumReaderBuilder.build(payload.writerSchema(), payload.readerSchema()));
    }

}
