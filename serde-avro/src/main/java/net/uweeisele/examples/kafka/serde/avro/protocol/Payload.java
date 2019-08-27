package net.uweeisele.examples.kafka.serde.avro.protocol;

import org.apache.kafka.common.header.Headers;

import java.util.function.Supplier;

public class Payload<T> implements Supplier<T> {

    private final String topic;

    private final Type type;

    private final Headers headers;

    private final T body;

    public Payload(Payload<? extends T> payload) {
        this(payload.topic, payload.type, payload.headers, payload.body);
    }

    public Payload(String topic, Type type, Headers headers, T body) {
        this.topic = topic;
        this.type = type;
        this.headers = headers;
        this.body = body;
    }

    public String topic() {
        return topic;
    }

    public Type type() {
        return type;
    }

    public Headers headers() {
        return headers;
    }

    @Override
    public T get() {
        return body;
    }

    public <V> Payload<V> withBody(V body) {
        return new Payload<>(topic, type, headers, body);
    }

    enum Type {

        KEY(true),
        VALUE(false);

        private final boolean isKey;

        Type(boolean isKey) {
            this.isKey = isKey;
        }

        public boolean isKey() {
            return isKey;
        }

        public static Type isKey(boolean isKey) {
            return isKey ? KEY : VALUE;
        }
    }
}
