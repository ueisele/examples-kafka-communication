package net.uweeisele.examples.kafka.serde.avro.deserializers;

import net.uweeisele.examples.kafka.serde.avro.deserializers.function.record.GenericDatumReaderBuilder;
import org.apache.avro.generic.GenericRecord;

public class GenericKafkaAvroRecordDeserializer extends KafkaAvroRecordDeserializer<GenericRecord> {

    public GenericKafkaAvroRecordDeserializer() {
        super(new GenericDatumReaderBuilder<>());
    }

}
