package net.uweeisele.examples.kafka.transformer;

public class ApplicationException extends RuntimeException {

    private final int errorCode;
    private final boolean logException;

    public ApplicationException(String message, Throwable cause, int errorCode, boolean logException) {
        super(message, cause);
        this.errorCode = errorCode;
        this.logException = logException;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public boolean isLogException() {
        return logException;
    }

    @Override
    public String toString() {
        return getClass().getName() + "{" +
                "message=\"" + getLocalizedMessage() + "\"" +
                ", errorCode=" + errorCode +
                ", logException=" + logException +
                ", cause=\"" + getCause() + "\"" +
                '}';
    }
}
