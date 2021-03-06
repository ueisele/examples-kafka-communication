package net.uweeisele.examples.kafka.transformer;

import java.util.Properties;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class RequiredValueBuilder implements Function<Properties, String> {

    private final String key;
    private final Function<String, ? extends RuntimeException> missingExceptionBuilder;

    public RequiredValueBuilder(String key) {
        this(key, IllegalArgumentException::new);
    }

    public RequiredValueBuilder(String key, Function<String, ? extends RuntimeException> missingExceptionBuilder) {
        this.key = requireNonNull(key);
        this.missingExceptionBuilder = requireNonNull(missingExceptionBuilder);
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
