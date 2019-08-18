package net.uweeisele.examples.kafka.serde.avro.deserializers.payload;

import org.apache.kafka.common.header.Headers;

import java.nio.ByteBuffer;

import static java.lang.String.format;

public class DeserializablePayload extends Payload implements TopicAware, HeadersAware, DataAware {

    private static final Key<String> KEY_TOPIC = new Key<>("payload.deserializable.topic");
    private static final Key<Boolean> KEY_IS_KEY = new Key<>("payload.deserializable.isKey");
    private static final Key<Headers> KEY_HEADERS = new Key<>("payload.deserializable.headers", v -> format("{size=%d}", v.toArray().length));
    private static final Key<ByteBuffer> KEY_DATA = new Key<>("payload.deserializable.data", v -> format("{length=%d}", v.limit()));

    public DeserializablePayload() {
    }

    public DeserializablePayload(PayloadAware payloadAware) {
        super(payloadAware);
    }

    public DeserializablePayload(Payload payload) {
        super(payload);
    }

    @Override
    public String topic() {
        return get(KEY_TOPIC);
    }

    @Override
    public DeserializablePayload withTopic(String topic) {
        put(KEY_TOPIC, topic);
        return this;
    }

    @Override
    public boolean isKey() {
        return get(KEY_IS_KEY);
    }

    @Override
    public DeserializablePayload withIsKey(boolean isKey) {
        put(KEY_IS_KEY, isKey);
        return this;
    }

    @Override
    public Headers headers() {
        return get(KEY_HEADERS);
    }

    @Override
    public DeserializablePayload withHeaders(Headers headers) {
        put(KEY_HEADERS, headers);
        return this;
    }

    @Override
    public ByteBuffer data() {
        return get(KEY_DATA);
    }

    @Override
    public DeserializablePayload withData(byte[] data) {
        return withData(data != null ? ByteBuffer.wrap(data) : null);
    }

    @Override
    public DeserializablePayload withData(ByteBuffer data) {
        put(KEY_DATA, data);
        return this;
    }

}
