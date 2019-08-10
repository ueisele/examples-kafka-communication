package net.uweeisele.examples.kafka.transformer;

import java.util.Properties;
import java.util.function.Function;

public class RequiredValueBuilder implements Function<Properties, String> {

    private final String key;
    private final Function<String, ? extends RuntimeException> missingExceptionBuilder;

    public RequiredValueBuilder(String key) {
        this(key, IllegalArgumentException::new);
    }

    public RequiredValueBuilder(String key, Function<String, ? extends RuntimeException> missingExceptionBuilder) {
        this.key = key;
        this.missingExceptionBuilder = missingExceptionBuilder;
    }

    @Override
    public String apply(Properties properties) {
        String value = properties.getProperty(key);
        if (value == null) {
            throw missingExceptionBuilder.apply(String.format("Missing required property %s", key));
        }
        return value;
    }
}
