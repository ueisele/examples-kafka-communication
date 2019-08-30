package net.uweeisele.examples.kafka.serde.avro.protocol.matcher;

import java.util.function.Predicate;
import java.util.regex.Pattern;

public class PatternPredicate implements Predicate<String> {

    private final Pattern pattern;

    public PatternPredicate(Pattern pattern) {
        this.pattern = pattern;
    }

    @Override
    public boolean test(String name) {
        return pattern.matcher(name).matches();
    }
}
