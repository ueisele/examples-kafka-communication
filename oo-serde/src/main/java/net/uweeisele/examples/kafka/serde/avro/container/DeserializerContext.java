package net.uweeisele.examples.kafka.serde.avro.container;

import net.uweeisele.examples.kafka.serde.avro.container.aware.*;
import org.apache.avro.Schema;
import org.apache.kafka.common.header.Headers;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

public class DeserializerContext implements Container<DeserializerContext>, TopicAware, HeadersAware, DataAware, ReaderSchemaAware {

    @Override
    public DeserializerContext context() {
        return this;
    }

    @Override
    public ByteBuffer data() {
        return null;
    }

    @Override
    public DataAware withData(byte[] data) {
        return null;
    }

    @Override
    public DataAware withData(ByteBuffer data) {
        return null;
    }

    @Override
    public Headers headers() {
        return null;
    }

    @Override
    public HeadersAware withHeaders(Headers headers) {
        return null;
    }

    @Override
    public Schema readerSchema() {
        return null;
    }

    @Override
    public ReaderSchemaAware withReaderSchema(Schema readerSchema) {
        return null;
    }

    @Override
    public Schema computeReaderSchemaIfAbsent(Supplier<Schema> supplier) {
        return null;
    }

    @Override
    public String topic() {
        return null;
    }

    @Override
    public TopicAware withTopic(String topic) {
        return null;
    }

    @Override
    public boolean isKey() {
        return false;
    }

    @Override
    public TopicAware withIsKey(boolean isKey) {
        return null;
    }

    @Override
    public Properties configs() {
        return null;
    }

    @Override
    public PayloadAware setConfigs(Properties configs) {
        return null;
    }

    @Override
    public Properties attributes() {
        return null;
    }

    @Override
    public PayloadAware setAttributes(Properties attributes) {
        return null;
    }

    @Override
    public PayloadAware withAttributes(Properties attributes) {
        return null;
    }

    @Override
    public PayloadAware withAttribute(String key, String value) {
        return null;
    }

    @Override
    public Map<String, String> printablePayload() {
        return null;
    }
}
