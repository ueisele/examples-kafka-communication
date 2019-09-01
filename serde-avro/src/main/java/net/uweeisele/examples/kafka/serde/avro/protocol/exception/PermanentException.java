package net.uweeisele.examples.kafka.serde.avro.protocol.exception;

import net.uweeisele.examples.kafka.serde.avro.protocol.ContextSupplier;

public class PermanentException extends SerdeException {

    public PermanentException(String message) {
        super(message);
    }

    public PermanentException(String message, ContextSupplier contextSupplier) {
        super(message, contextSupplier);
    }

    public PermanentException(String message, Throwable cause) {
        super(message, cause);
    }

    public PermanentException(String message, Throwable cause, ContextSupplier contextSupplier) {
        super(message, cause, contextSupplier);
    }
}
