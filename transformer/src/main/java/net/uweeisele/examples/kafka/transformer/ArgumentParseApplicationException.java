package net.uweeisele.examples.kafka.transformer;

import static net.uweeisele.examples.kafka.transformer.ErrorCode.Severity.INFO;
import static net.uweeisele.examples.kafka.transformer.ReturnCode.internalCode;

public class ArgumentParseApplicationException extends ApplicationException {

    private static final ErrorCode ERROR_CODE = new ErrorCode(
            "Could not parse a given argument. %s",
            internalCode(128), INFO, true);

    public ArgumentParseApplicationException(Throwable cause) {
        this("", cause);
    }

    public ArgumentParseApplicationException(String message, Throwable cause) {
        super(ERROR_CODE.withFormattedMessage(message), cause);
    }
}
