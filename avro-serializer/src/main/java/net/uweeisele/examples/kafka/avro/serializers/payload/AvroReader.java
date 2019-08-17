package net.uweeisele.examples.kafka.avro.serializers.payload;

import net.uweeisele.examples.kafka.avro.serializers.payload.Payload.Key;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;

public interface AvroReader<D extends IndexedRecord> extends PayloadContainer {

    DatumReader<D> avroReader();

    AvroReader<D> withAvroReader(DatumReader<D> avroReader);

    static <D extends IndexedRecord> Key<DatumReader<D>> avroReaderKey() {
        return new Key<>(AvroReader.class.getSimpleName());
    }
}
