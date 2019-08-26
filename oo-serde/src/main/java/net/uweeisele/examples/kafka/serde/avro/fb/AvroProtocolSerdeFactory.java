package net.uweeisele.examples.kafka.serde.avro.fb;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.*;
import org.apache.kafka.common.errors.SerializationException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class AvroProtocolSerdeFactory {


    public static class AvroSerializer {

        private static final byte MAGIC_BYTE_VALUE = 0;

        private static final byte MAGIC_BYTE_INDEX = 0;
        private static final byte SCHEMA_ID_INDEX = 1;
        private static final byte CONTENT_INDEX = 5;

        private final EncoderFactory encoderFactory = EncoderFactory.get();

        private Supplier<Integer> schemaIdSupplier;
        private Consumer<Encoder> contentWriter;

        public byte[] build() throws IOException {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(MAGIC_BYTE_VALUE);
            out.write(ByteBuffer.allocate(4).putInt(schemaIdSupplier.get()).array());
            BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
            contentWriter.accept(encoder);
            encoder.flush();
            return out.toByteArray();
        }

        public void setSchemaIdSupplier(Supplier<Integer> schemaIdSupplier) {
            this.schemaIdSupplier = schemaIdSupplier;
        }

        public void setContentWriter(Consumer<Encoder> contentWriter) {
            this.contentWriter = contentWriter;
        }
    }

    public static class Serializer {

        private static final byte MAGIC_BYTE_VALUE = 0;

        private static final byte MAGIC_BYTE_INDEX = 0;
        private static final byte SCHEMA_ID_INDEX = 1;
        private static final byte CONTENT_INDEX = 5;

        private final EncoderFactory encoderFactory = EncoderFactory.get();

        private Supplier<Integer> schemaIdSupplier;
        private Consumer<Encoder> contentWriter;

        public byte[] build() throws IOException {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(MAGIC_BYTE_VALUE);
            out.write(ByteBuffer.allocate(4).putInt(schemaIdSupplier.get()).array());
            BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
            contentWriter.accept(encoder);
            encoder.flush();
            return out.toByteArray();
        }

        public void setSchemaIdSupplier(Supplier<Integer> schemaIdSupplier) {
            this.schemaIdSupplier = schemaIdSupplier;
        }

        public void setContentWriter(Consumer<Encoder> contentWriter) {
            this.contentWriter = contentWriter;
        }
    }

    public static class Deserializer<T> {

        private static final byte MAGIC_BYTE_VALUE = 0;

        private static final byte MAGIC_BYTE_INDEX = 0;
        private static final byte SCHEMA_ID_INDEX = 1;
        private static final byte CONTENT_INDEX = 5;

        private final DecoderFactory decoderFactory = DecoderFactory.get();

        private final byte[] data;

        private Function<Integer, Schema> schemaResolver;
        private BiFunction<Schema, Decoder, T> contentReader;

        public Deserializer(byte[] data) {
            this.data = data;
        }

        public T build() throws IOException {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            if (buffer.get() != MAGIC_BYTE_VALUE)
                throw new SerializationException("");
            Schema schema = schemaResolver.apply(buffer.getInt());
            int start = buffer.position() + buffer.arrayOffset();
            int length = buffer.limit() - buffer.position();
            BinaryDecoder decoder = decoderFactory.binaryDecoder(buffer.array(), start, length, null);
            return contentReader.apply(schema, decoder);
        }

        public void setSchemaResolver(Function<Integer, Schema> schemaResolver) {
            this.schemaResolver = schemaResolver;
        }

        public void setContentReader(BiFunction<Schema, Decoder, T> contentReader) {
            this.contentReader = contentReader;
        }
    }

    public static class AvroRecordDeserializer<T extends IndexedRecord> {

        private static final byte MAGIC_BYTE_VALUE = 0;

        private static final int MAGIC_BYTE_INDEX = 0;
        private static final int MAGIC_BYTE_BYTES = 1;
        private static final int SCHEMA_ID_INDEX = 1;
        private static final int SCHEMA_ID_BYTES = 4;
        private static final int CONTENT_INDEX = MAGIC_BYTE_BYTES + SCHEMA_ID_BYTES;

        private final DecoderFactory decoderFactory = DecoderFactory.get();

        protected final Config config;

        protected final Payload<byte[]> payload;
        protected final ByteBuffer inputBuffer;
        protected final Decoder decoder;

        protected Function<Integer, Schema> schemaResolver;
        protected BiFunction<Schema, Decoder, T> contentReader;

        public AvroRecordDeserializer(Config config, Payload<byte[]> payload) {
            this.config = config;
            this.payload = payload;
            this.inputBuffer = ByteBuffer.wrap(payload.get());
            this.decoder = decoderFactory.binaryDecoder(payload.get(), CONTENT_INDEX, payload.get().length - CONTENT_INDEX, null);
        }

        public Payload<T> deserialize() {
            if (inputBuffer.get(MAGIC_BYTE_INDEX) != MAGIC_BYTE_VALUE)
                throw new SerializationException("No Magic Byte");
            Schema schema = schemaResolver.apply(inputBuffer.getInt(SCHEMA_ID_INDEX));
            T object = contentReader.apply(schema, decoder);
            return payload.withBody(object);
        }

        public void setSchemaResolver(Function<Integer, Schema> schemaResolver) {
            this.schemaResolver = schemaResolver;
        }

        public void setContentReader(BiFunction<Schema, Decoder, T> contentReader) {
            this.contentReader = contentReader;
        }
    }

    public static class AvroProtocolDecoder {

        private static final byte MAGIC_BYTE_VALUE = 0;

        private static final int MAGIC_BYTE_INDEX = 0;
        private static final int MAGIC_BYTE_BYTES = 1;
        private static final int SCHEMA_ID_INDEX = 1;
        private static final int SCHEMA_ID_BYTES = 4;
        private static final int CONTENT_INDEX = MAGIC_BYTE_BYTES + SCHEMA_ID_BYTES;

        private final DecoderFactory decoderFactory = DecoderFactory.get();

        private final Config config;

        private final Payload<byte[]> payload;
        private final ByteBuffer inputBuffer;
        private final Decoder decoder;

        public AvroProtocolDecoder(Config config, Payload<byte[]> payload) {
            this.config = config;
            this.payload = payload;
            this.inputBuffer = ByteBuffer.wrap(payload.get());
            this.decoder = decoderFactory.binaryDecoder(payload.get(), CONTENT_INDEX, payload.get().length - CONTENT_INDEX, null);
        }

        public byte magicByte() {
            return inputBuffer.get(MAGIC_BYTE_INDEX);
        }

        public int schemaId() {
            return inputBuffer.getInt(SCHEMA_ID_INDEX);
        }

        public Decoder contentDecoder() {
            return decoder;
        }

    }

    public static class AvroProtocol {

        private static final byte MAGIC_BYTE_VALUE = 0;

        private static final int MAGIC_BYTE_INDEX = 0;
        private static final int MAGIC_BYTE_BYTES = 1;
        private static final int SCHEMA_ID_INDEX = 1;
        private static final int SCHEMA_ID_BYTES = 4;
        private static final int CONTENT_INDEX = MAGIC_BYTE_BYTES + SCHEMA_ID_BYTES;

        private final DecoderFactory decoderFactory = DecoderFactory.get();
        private final EncoderFactory encoderFactory = EncoderFactory.get();

        private final Config config;

        public AvroProtocol(Config config) {
            this.config = config;
        }

        public ProtocolDecoder<Integer, Decoder> decoder(Payload<byte[]> input) {
            ByteBuffer inputBuffer = ByteBuffer.wrap(input.get());
            if (inputBuffer.get(MAGIC_BYTE_INDEX) != MAGIC_BYTE_VALUE)
                throw new SerializationException("No Magic Byte");
            return new ProtocolDecoder<>() {

                @Override
                public Integer schema() {
                    return inputBuffer.getInt(SCHEMA_ID_INDEX);
                }

                @Override
                public Decoder content() {
                    return decoderFactory.binaryDecoder(input.get(), CONTENT_INDEX, input.get().length - CONTENT_INDEX, null);
                }
            };
        }

        public ProtocolEncoder<Integer, Consumer<Encoder>> encoder(Payload<?> input) {
            return new ProtocolEncoder<>() {

                Integer schemaId;
                Consumer<Encoder> encoderAction;

                @Override
                public ProtocolEncoder<Integer, Consumer<Encoder>> withSchema(Integer schemaId) {
                    this.schemaId = schemaId;
                    return this;
                }

                @Override
                public ProtocolEncoder<Integer, Consumer<Encoder>> withContent(Consumer<Encoder> encoderAction) {
                    this.encoderAction = encoderAction;
                    return this;
                }

                @Override
                public Payload<byte[]> build() throws IOException {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    out.write(MAGIC_BYTE_VALUE);
                    out.write(ByteBuffer.allocate(SCHEMA_ID_BYTES).putInt(schemaId).array());
                    BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
                    encoderAction.accept(encoder);
                    encoder.flush();
                    return input.withBody(out.toByteArray());
                }
            };
        }

    }

    public interface ProtocolDecoder<S, C> {

        S schema();

        C content();
    }

    public interface ProtocolEncoder<S, C> {

        ProtocolEncoder<S, C> withSchema(S schema);

        ProtocolEncoder<S, C> withContent(C content);

        Payload<byte[]> build() throws IOException;
    }

}
