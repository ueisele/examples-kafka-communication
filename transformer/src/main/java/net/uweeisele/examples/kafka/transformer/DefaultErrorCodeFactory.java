package net.uweeisele.examples.kafka.transformer;

import java.util.function.Function;

import static net.uweeisele.examples.kafka.transformer.ErrorCode.Severity.ERROR;
import static net.uweeisele.examples.kafka.transformer.ErrorCode.Severity.WARNING;
import static net.uweeisele.examples.kafka.transformer.ReturnCode.internalCode;

public class DefaultErrorCodeFactory implements Function<Throwable, ErrorCode> {

    private static final ErrorCode UNEXPECTED_EXCEPTION = new ErrorCode(
            "Unexpected exception occurred which caused abnormal termination of this application!",
            internalCode(254), ERROR, false);

    private static final ErrorCode ILLEGAL_ARGUMENT = new ErrorCode(
            "Given arguments are not valid. %s",
            internalCode(253), WARNING, false);

    @Override
    public ErrorCode apply(Throwable e) {
        if (e instanceof ApplicationException) {
            return ((ApplicationException) e).getErrorCode();
        }
        if (e instanceof IllegalArgumentException) {
            return ILLEGAL_ARGUMENT.withFormattedMessage(e.getLocalizedMessage());
        }
        return UNEXPECTED_EXCEPTION;
    }
}
