package net.uweeisele.examples.kafka.transformer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class ListValueBuilder implements Function<Properties, List<String>> {

    private static final String DEFAULT_DELIMITER = ",";

    private final Function<Properties, String> valueBuilder;
    private final String delimiter;

    public ListValueBuilder(Function<Properties, String> valueBuilder) {
        this(valueBuilder, DEFAULT_DELIMITER);
    }

    public ListValueBuilder(Function<Properties, String> valueBuilder, String delimiter) {
        this.valueBuilder = requireNonNull(valueBuilder);
        this.delimiter = requireNonNull(delimiter);
    }

    @Override
    public List<String> apply(Properties properties) {
        String value = valueBuilder.apply(properties);
        return new ArrayList<>(asList(value.split(delimiter)));
    }
}
