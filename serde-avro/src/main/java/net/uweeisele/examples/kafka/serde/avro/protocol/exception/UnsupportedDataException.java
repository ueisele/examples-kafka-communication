package net.uweeisele.examples.kafka.serde.avro.protocol.exception;

import net.uweeisele.examples.kafka.serde.avro.protocol.ContextSupplier;

public class UnsupportedDataException extends PermanentException {

    public UnsupportedDataException(String message) {
        super(message);
    }

    public UnsupportedDataException(String message, ContextSupplier contextSupplier) {
        super(message, contextSupplier);
    }

    public UnsupportedDataException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnsupportedDataException(String message, Throwable cause, ContextSupplier contextSupplier) {
        super(message, cause, contextSupplier);
    }
}
