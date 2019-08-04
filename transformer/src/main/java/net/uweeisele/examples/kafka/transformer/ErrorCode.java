package net.uweeisele.examples.kafka.transformer;

import static java.util.Objects.requireNonNull;

public class ErrorCode {

    public enum Severity {
        ERROR,
        WARNING,
        INFO
    }

    private final String message;
    private final ReturnCode code;

    private final Severity severity;
    private final boolean suppressMessage;

    public ErrorCode(String message, ReturnCode code, Severity severity) {
        this(message, code, severity, false);
    }

    public ErrorCode(String message, ReturnCode code, Severity severity, boolean suppressMessage) {
        this.message = requireNonNull(message);
        this.code = requireNonNull(code);
        this.severity = requireNonNull(severity);
        this.suppressMessage = suppressMessage;
    }

    public String getMessage() {
        return message;
    }

    public ReturnCode getCode() {
        return code;
    }

    public Severity getSeverity() {
        return severity;
    }

    public boolean isSuppressMessage() {
        return suppressMessage;
    }

    public ErrorCode withMessage(String message) {
        return new ErrorCode(message, code, severity, suppressMessage);
    }

    public ErrorCode withCode(ReturnCode code) {
        return new ErrorCode(message, code, severity, suppressMessage);
    }

    public ErrorCode withSeverity(Severity severity) {
        return new ErrorCode(message, code, severity, suppressMessage);
    }

    public ErrorCode withSuppressMessage(boolean suppressMessage) {
        return new ErrorCode(message, code, severity, suppressMessage);
    }

}
