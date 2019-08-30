package net.uweeisele.examples.kafka.serde.avro.protocol.avro;

import net.uweeisele.examples.kafka.serde.avro.protocol.Payload;
import net.uweeisele.examples.kafka.serde.avro.protocol.Protocol;
import net.uweeisele.examples.kafka.serde.avro.protocol.ProtocolContainer;
import net.uweeisele.examples.kafka.serde.avro.protocol.ProtocolDecoder;
import net.uweeisele.examples.kafka.serde.avro.protocol.avro.schema.SchemaResolver;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;

import java.io.IOException;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

public class AvroSchemaProtocolDecoder implements ProtocolDecoder<Protocol<Integer, Decoder>, Protocol<Schema, Decoder>> {

    private final Properties properties;

    private final SchemaResolver schemaResolver;

    public AvroSchemaProtocolDecoder(Properties properties, SchemaResolver schemaResolver) {
        this.properties = requireNonNull(properties);
        this.schemaResolver = requireNonNull(schemaResolver);
    }

    @Override
    public Payload<Protocol<Schema, Decoder>> decode(Payload<Protocol<Integer, Decoder>> input) {
        return input.withBody(decode(input.get().schema(), input.get().content()));
    }

    private Protocol<Schema, Decoder> decode(Integer writerSchemaId, Decoder decoder) {
        Schema writerSchema = schemaResolver.getSchemaById(writerSchemaId);
        return new ProtocolContainer<>(writerSchema, decoder);
    }
}
