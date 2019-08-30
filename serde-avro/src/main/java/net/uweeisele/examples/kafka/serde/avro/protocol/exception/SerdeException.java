package net.uweeisele.examples.kafka.serde.avro.protocol.exception;

import java.util.HashMap;
import java.util.Map;

public class SerdeException extends RuntimeException {

    private final Map<String, Object> attributes = new HashMap<>();

    public SerdeException(String message) {
        this(message, new HashMap<>());
    }

    public SerdeException(String message, Map<String, Object> attributes) {
        this(message, null, attributes);
    }

    public SerdeException(String message, Throwable cause) {
        this(message, cause, new HashMap<>());
    }

    public SerdeException(String message, Throwable cause, Map<String, Object> attributes) {
        super(message, cause);
        if (cause instanceof SerdeException) {
            this.attributes.putAll(((SerdeException) cause).getAttributes());
        }
        this.attributes.putAll(attributes);
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public SerdeException withAttributes(Map<String, Object> attributes) {
        this.attributes.putAll(attributes);
        return this;
    }

    public SerdeException withAttribute(String key, Object value) {
        this.attributes.put(key, value);
        return this;
    }

    public <T> T getAttribute(String key, Class<T> type) {
        Object value = attributes.get(key);
        if (value != null && !type.isAssignableFrom(value.getClass())) {
            return null;
        }
        return (T) value;
    }
}
