package net.uweeisele.examples.kafka.avro.serializers.payload;

import net.uweeisele.examples.kafka.avro.serializers.extractor.deserializer.SchemaAwareDatumReader;
import net.uweeisele.examples.kafka.avro.serializers.payload.Payload.Key;
import org.apache.avro.generic.IndexedRecord;

import java.util.function.Consumer;

import static net.uweeisele.examples.kafka.avro.serializers.payload.AvroReader.avroReaderKey;

public class AvroBytesReaderPayload<D extends IndexedRecord> extends AvroBytesWriterReaderSchemaPayload implements AvroReader<D> {

    private final Key<SchemaAwareDatumReader<D>> avroReaderKey;

    public <T extends AvroBytes & WriterSchema> AvroBytesReaderPayload(T payload) {
        super(payload);
        this.avroReaderKey = avroReaderKey();
    }

    @Override
    public AvroBytesReaderPayload<D> withAvroReader(SchemaAwareDatumReader<D> avroReader) {
        payload.put(avroReaderKey, avroReader);
        withWriterSchema(avroReader.getSchema());
        withReaderSchema(avroReader.getExpected());
        return this;
    }

    @Override
    public AvroBytesReaderPayload<D> with(Consumer<Payload> action) {
        action.accept(payload());
        return this;
    }

    @Override
    public SchemaAwareDatumReader<D> avroReader() {
        return avroReaderKey.cast(payload.get(avroReaderKey));
    }
}
