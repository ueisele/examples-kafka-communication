package net.uweeisele.examples.kafka.serde.avro.protocol.kafkaavro;

public interface KafkaAvroProtocolConstants {

    String NAME = "KafkaAvro";

    int MAGIC_BYTE_INDEX = 0;
    int MAGIC_BYTE_BYTES = 1;
    byte MAGIC_BYTE_VALUE = 0;

    int SCHEMA_ID_INDEX = 1;
    int SCHEMA_ID_BYTES = 4;

    int CONTENT_INDEX = MAGIC_BYTE_BYTES + SCHEMA_ID_BYTES;
}
