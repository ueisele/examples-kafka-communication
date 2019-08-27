package net.uweeisele.examples.kafka.serde.avro.protocol.avro;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import net.uweeisele.examples.kafka.serde.avro.protocol.Payload;
import net.uweeisele.examples.kafka.serde.avro.protocol.Protocol;
import net.uweeisele.examples.kafka.serde.avro.protocol.ProtocolDecoder;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.Decoder;

import java.util.Properties;

public class AvroProtocolDecoder<T extends GenericContainer> implements ProtocolDecoder<Protocol<Integer, Decoder>, T> {

    private final Properties properties;

    private final SchemaRegistryClient schemaRegistryClient;

    public AvroProtocolDecoder(Properties properties, SchemaRegistryClient schemaRegistryClient) {
        this.properties = properties;
        this.schemaRegistryClient = schemaRegistryClient;
    }

    @Override
    public Payload<T> decode(Payload<Protocol<Integer, Decoder>> input) {
        return null;
    }
}
