package net.uweeisele.examples.kafka.serde.avro.protocol.exception;

import net.uweeisele.examples.kafka.serde.avro.protocol.ContextSupplier;

public class UnexpectedDataException extends PermanentException {

    public UnexpectedDataException(String message) {
        super(message);
    }

    public UnexpectedDataException(String message, ContextSupplier contextSupplier) {
        super(message, contextSupplier);
    }

    public UnexpectedDataException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnexpectedDataException(String message, Throwable cause, ContextSupplier contextSupplier) {
        super(message, cause, contextSupplier);
    }
}
