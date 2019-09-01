package net.uweeisele.examples.kafka.serde.avro.protocol.exception;

import net.uweeisele.examples.kafka.serde.avro.protocol.ContextSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

public class SerdeException extends RuntimeException implements ContextSupplier {

    private final List<ContextSupplier> contextSuppliers = new ArrayList<>();

    public SerdeException(String message) {
        this(message, (Throwable) null);
    }

    public SerdeException(String message, ContextSupplier contextSupplier) {
        this(message, null, contextSupplier);
    }

    public SerdeException(String message, Throwable cause) {
        this(message, cause, null);
    }

    public SerdeException(String message, Throwable cause, ContextSupplier contextSupplier) {
        super(message, cause);
        if (cause instanceof ContextSupplier) {
            addContextSupplier(((ContextSupplier) cause));
        }
        addContextSupplier(contextSupplier);
    }

    @Override
    public Map<String, String> context() {
        return contextSuppliers.stream()
                .flatMap(contextSupplier -> contextSupplier.context().entrySet().stream())
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }


    public SerdeException addContext(Map<String, String> context) {
        return addContextSupplier(() -> context);
    }

    public SerdeException addContextSupplier(ContextSupplier contextSupplier) {
        if (contextSupplier != null) {
            this.contextSuppliers.add(contextSupplier);
        }
        return this;
    }

}
