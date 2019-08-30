package net.uweeisele.examples.kafka.serde.avro.protocol.exception;

import java.util.Map;

public class DeserializationException extends SerdeException {

    public DeserializationException(String message) {
        super(message);
    }

    public DeserializationException(String message, Map<String, Object> attributes) {
        super(message, attributes);
    }

    public DeserializationException(String message, Throwable cause) {
        super(message, cause);
    }

    public DeserializationException(String message, Throwable cause, Map<String, Object> attributes) {
        super(message, cause, attributes);
    }
}
