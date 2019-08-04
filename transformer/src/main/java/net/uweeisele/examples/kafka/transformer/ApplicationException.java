package net.uweeisele.examples.kafka.transformer;

import static java.util.Objects.requireNonNull;

public class ApplicationException extends RuntimeException {

    private final ReturnCode.Public code;
    private final boolean suppressMessage;

    public ApplicationException(String message, ReturnCode.Public code, boolean suppressMessage) {
        this(message, null, code, suppressMessage);
    }

    public ApplicationException(String message, Throwable cause, ReturnCode.Public code, boolean suppressMessage) {
        super(message, cause);
        this.code = requireNonNull(code);
        this.suppressMessage = suppressMessage;
    }

    public ReturnCode.Public getCode() {
        return code;
    }

    public boolean isSuppressMessage() {
        return suppressMessage;
    }

    @Override
    public String toString() {
        return getClass().getName() + "{" +
                "message=\"" + getLocalizedMessage() + "\"" +
                ", code=" + code.get() +
                ", suppressMessage=" + suppressMessage +
                ", cause=\"" + getCause() + "\"" +
                '}';
    }
}
