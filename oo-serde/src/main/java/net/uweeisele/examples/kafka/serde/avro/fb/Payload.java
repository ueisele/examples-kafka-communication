package net.uweeisele.examples.kafka.serde.avro.fb;

import org.apache.kafka.common.header.Headers;

import java.util.function.Supplier;

public class Payload<T> implements Supplier<T> {

    private final String topic;

    private final Headers headers;

    private final T body;

    public Payload(Payload<? extends T> payload) {
        this.topic = payload.topic;
        this.headers = payload.headers;
        this.body = payload.body;
    }

    public Payload(String topic, Headers headers, T body) {
        this.topic = topic;
        this.headers = headers;
        this.body = body;
    }

    public String topic() {
        return topic;
    }

    public Headers headers() {
        return headers;
    }

    @Override
    public T get() {
        return body;
    }

    public <V> Payload<V> withBody(V body) {
        return new Payload<>(topic, headers, body);
    }

}
