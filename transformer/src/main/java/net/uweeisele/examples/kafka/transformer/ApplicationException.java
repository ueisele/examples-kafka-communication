package net.uweeisele.examples.kafka.transformer;

import static java.util.Objects.requireNonNull;

public class ApplicationException extends RuntimeException {

    private final ErrorCode errorCode;

    public ApplicationException(ErrorCode errorCode) {
        this(errorCode, null);
    }

    public ApplicationException(ErrorCode errorCode, Throwable cause) {
        super(errorCode.getMessage(), cause);
        this.errorCode = requireNonNull(errorCode);
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    @Override
    public String toString() {
        return getClass().getName() + "{" +
                "errorCode=" + errorCode +
                ", cause=\"" + getCause() + "\"" +
                '}';
    }
}
