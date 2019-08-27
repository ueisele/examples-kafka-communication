package net.uweeisele.examples.kafka.serde.avro.protocol.kafkaavro;

import net.uweeisele.examples.kafka.serde.avro.protocol.Payload;
import net.uweeisele.examples.kafka.serde.avro.protocol.Protocol;
import net.uweeisele.examples.kafka.serde.avro.protocol.ProtocolDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.errors.SerializationException;

import java.nio.ByteBuffer;
import java.util.Properties;

import static java.util.Objects.requireNonNull;
import static net.uweeisele.examples.kafka.serde.avro.protocol.kafkaavro.KafkaAvroProtocolConstants.*;

public class KafkaAvroProtocolDecoder implements ProtocolDecoder<byte[], Protocol<Integer, Decoder>> {

    private final Properties properties;

    private final DecoderFactory decoderFactory;

    public KafkaAvroProtocolDecoder(Properties properties) {
        this(properties, DecoderFactory.get());
    }

    KafkaAvroProtocolDecoder(Properties properties, DecoderFactory decoderFactory) {
        this.properties = requireNonNull(properties);
        this.decoderFactory = requireNonNull(decoderFactory);
    }

    @Override
    public Payload<Protocol<Integer, Decoder>> decode(Payload<byte[]> input) {
        return input.withBody(new KafkaAvroProtocol(input));
    }

    class KafkaAvroProtocol implements Protocol<Integer, Decoder> {

        private final Payload<byte[]> input;
        private final ByteBuffer inputBuffer;

        KafkaAvroProtocol(Payload<byte[]> input) {
            this.input = input;
            this.inputBuffer = ByteBuffer.wrap(input.get());
            assertIsKafkaAvro();
        }

        private void assertIsKafkaAvro() {
            if (inputBuffer.get(MAGIC_BYTE_INDEX) != MAGIC_BYTE_VALUE) {
                throw new SerializationException("No Magic Byte");
            }
        }

        @Override
        public Integer schema() {
            return inputBuffer.getInt(SCHEMA_ID_INDEX);
        }

        @Override
        public Decoder content() {
            return decoderFactory.binaryDecoder(input.get(), CONTENT_INDEX, input.get().length - CONTENT_INDEX, null);
        }
    }
}
