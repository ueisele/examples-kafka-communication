package net.uweeisele.examples.kafka.avro.serializers.payload;

import net.uweeisele.examples.kafka.avro.serializers.payload.Payload.Key;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;

import static net.uweeisele.examples.kafka.avro.serializers.payload.AvroReader.avroReaderKey;

public class AvroBytesReaderPayload<D extends IndexedRecord> extends AvroBytesPayload implements AvroReader<D> {

    private final Key<DatumReader<D>> avroReaderKey;

    public AvroBytesReaderPayload(AvroBytes payload) {
        super(payload.payload());
        this.avroReaderKey = avroReaderKey();
    }

    @Override
    public AvroBytesReaderPayload<D> withAvroReader(DatumReader<D> avroReader) {
        payload.put(avroReaderKey, avroReader);
        return this;
    }

    @Override
    public DatumReader<D> avroReader() {
        return avroReaderKey.cast(payload.get(avroReaderKey));
    }
}
