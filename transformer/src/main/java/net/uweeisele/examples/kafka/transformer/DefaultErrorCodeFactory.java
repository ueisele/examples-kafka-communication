package net.uweeisele.examples.kafka.transformer;

import java.util.function.Function;

import static net.uweeisele.examples.kafka.transformer.ErrorCode.Severity.ERROR;
import static net.uweeisele.examples.kafka.transformer.ErrorCode.Severity.INFO;
import static net.uweeisele.examples.kafka.transformer.ReturnCode.internalCode;

public class DefaultErrorCodeFactory implements Function<Exception, ErrorCode> {

    private static final ErrorCode UNEXPECTED_EXCEPTION = new ErrorCode(
            "Unexpected exception occurred which caused abnormal termination of this application!",
            internalCode(255), ERROR, false);

    @Override
    public ErrorCode apply(Exception e) {
        if (e instanceof ApplicationException) {
            ApplicationException appException = (ApplicationException) e;
            return new ErrorCode(e.getLocalizedMessage(), appException.getCode(), INFO, appException.isSuppressMessage());
        }
        return UNEXPECTED_EXCEPTION;
    }
}
