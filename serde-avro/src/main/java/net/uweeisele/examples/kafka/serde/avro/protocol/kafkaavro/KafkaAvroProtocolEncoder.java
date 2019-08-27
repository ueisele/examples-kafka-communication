package net.uweeisele.examples.kafka.serde.avro.protocol.kafkaavro;

import net.uweeisele.examples.kafka.serde.avro.protocol.Payload;
import net.uweeisele.examples.kafka.serde.avro.protocol.Protocol;
import net.uweeisele.examples.kafka.serde.avro.protocol.ProtocolEncoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static net.uweeisele.examples.kafka.serde.avro.protocol.kafkaavro.KafkaAvroProtocolConstants.MAGIC_BYTE_VALUE;
import static net.uweeisele.examples.kafka.serde.avro.protocol.kafkaavro.KafkaAvroProtocolConstants.SCHEMA_ID_BYTES;

public class KafkaAvroProtocolEncoder implements ProtocolEncoder<Protocol<Integer, Consumer<Encoder>>, byte[]> {

    private final Properties properties;

    private final EncoderFactory encoderFactory;

    public KafkaAvroProtocolEncoder(Properties properties) {
        this(properties, EncoderFactory.get());
    }

    KafkaAvroProtocolEncoder(Properties properties, EncoderFactory encoderFactory) {
        this.properties = requireNonNull(properties);
        this.encoderFactory = requireNonNull(encoderFactory);
    }

    @Override
    public Payload<byte[]> encode(Payload<Protocol<Integer, Consumer<Encoder>>> input) throws IOException {
        return input.withBody(encode(input.get().schema(), input.get().content()));
    }

    private byte[] encode(Integer schemaId, Consumer<Encoder> encoderAction) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(MAGIC_BYTE_VALUE);
        out.write(ByteBuffer.allocate(SCHEMA_ID_BYTES).putInt(schemaId).array());
        BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
        encoderAction.accept(encoder);
        encoder.flush();
        return out.toByteArray();
    }

}
