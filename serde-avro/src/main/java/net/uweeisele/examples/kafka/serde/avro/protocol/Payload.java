package net.uweeisele.examples.kafka.serde.avro.protocol;

import org.apache.kafka.common.header.Headers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toMap;
import static net.uweeisele.examples.kafka.serde.avro.protocol.ContextSupplierBuilder.contextSupplier;

public class Payload<T> implements Supplier<T>, ContextSupplier {

    private final String topic;

    private final Type type;

    private final Headers headers;

    private final T body;

    private final List<ContextSupplier> contextSuppliers;

    public Payload(Payload<? extends T> payload) {
        this(payload.topic, payload.type, payload.headers, payload.body, payload.contextSuppliers);
    }

    public Payload(String topic, Type type, Headers headers, T body) {
        this(topic, type, headers, body, new ArrayList<>());
    }

    Payload(String topic, Type type, Headers headers, T body, List<ContextSupplier> contextSuppliers) {
        this.topic = topic;
        this.type = type;
        this.headers = headers;
        this.body = body;
        this.contextSuppliers = contextSuppliers;
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

    public <V> Payload<V> withBody(V body, Function<V, ContextSupplier> contextSupplierFactory) {
        addContextSupplier(contextSupplierFactory.apply(body));
        return withBody(body);
    }

    public <V> Payload<V> withBody(V body) {
        return new Payload<>(topic, type, headers, body, contextSuppliers);
    }

    public <V extends ContextSupplier> Payload<V> withBody(V body) {
        addContextSupplier(body);
        return new Payload<>(topic, type, headers, body, contextSuppliers);
    }

    @Override
    public Map<String, String> context() {
        return contextSupplier()
                .withAll(contextSuppliers.stream()
                    .flatMap(contextSupplier -> contextSupplier.context().entrySet().stream())
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue)))
                .with("topic", topic)
                .with("dataType", type)
                .context();
    }

    public Payload<T> addContext(Map<String, String> context) {
        return addContextSupplier(() -> context);
    }

    public Payload<T> addContextSupplier(ContextSupplier contextSupplier) {
        this.contextSuppliers.add(contextSupplier);
        return this;
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
