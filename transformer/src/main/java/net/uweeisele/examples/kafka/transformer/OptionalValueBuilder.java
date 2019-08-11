package net.uweeisele.examples.kafka.transformer;

import java.util.Properties;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class OptionalValueBuilder implements Function<Properties, String> {

    private final String key;
    private final String defaultValue;

    public OptionalValueBuilder(String key) {
        this(key, null);
    }

    public OptionalValueBuilder(String key, String defaultValue) {
        this.key = requireNonNull(key);
        this.defaultValue = defaultValue;
    }

    @Override
    public String apply(Properties properties) {
        String value = properties.getProperty(key);
        if (value == null) {
            value = defaultValue;
        }
        return value;
    }
}
