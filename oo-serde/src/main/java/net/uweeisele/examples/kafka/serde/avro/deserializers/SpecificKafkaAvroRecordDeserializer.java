package net.uweeisele.examples.kafka.serde.avro.deserializers;

import net.uweeisele.examples.kafka.serde.avro.deserializers.function.record.SpecificDatumReaderBuilder;
import org.apache.avro.specific.SpecificRecord;

public class SpecificKafkaAvroRecordDeserializer<T extends SpecificRecord> extends KafkaAvroRecordDeserializer<T> {

    public SpecificKafkaAvroRecordDeserializer() {
        this((Class<? extends T>) SpecificRecord.class);
    }

    public SpecificKafkaAvroRecordDeserializer(Class<? extends T> type) {
        super(new SpecificDatumReaderBuilder<>(type));
    }

}
