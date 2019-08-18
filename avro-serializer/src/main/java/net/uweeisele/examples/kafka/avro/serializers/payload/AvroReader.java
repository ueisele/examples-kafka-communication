package net.uweeisele.examples.kafka.avro.serializers.payload;

import net.uweeisele.examples.kafka.avro.serializers.extractor.deserializer.SchemaAwareDatumReader;
import net.uweeisele.examples.kafka.avro.serializers.payload.Payload.Key;
import org.apache.avro.generic.IndexedRecord;

public interface AvroReader<D extends IndexedRecord> extends PayloadContainer {

    SchemaAwareDatumReader<D> avroReader();

    AvroReader<D> withAvroReader(SchemaAwareDatumReader<D> avroReader);

    static <D extends IndexedRecord> Key<SchemaAwareDatumReader<D>> avroReaderKey() {
        return new Key<>(AvroReader.class.getSimpleName());
    }
}
