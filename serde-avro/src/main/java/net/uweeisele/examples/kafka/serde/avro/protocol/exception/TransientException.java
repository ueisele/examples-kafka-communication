package net.uweeisele.examples.kafka.serde.avro.protocol.exception;

import net.uweeisele.examples.kafka.serde.avro.protocol.ContextSupplier;

public class TransientException extends SerdeException {

    public TransientException(String message) {
        super(message);
    }

    public TransientException(String message, ContextSupplier contextSupplier) {
        super(message, contextSupplier);
    }

    public TransientException(String message, Throwable cause) {
        super(message, cause);
    }

    public TransientException(String message, Throwable cause, ContextSupplier contextSupplier) {
        super(message, cause, contextSupplier);
    }
}
