package net.uweeisele.examples.kafka.serde.avro.protocol.matcher;

import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;
import static net.uweeisele.examples.kafka.serde.avro.protocol.matcher.SchemaMatcher.SchemaClassification.*;

public class PredicateSchemaMatcher implements SchemaMatcher {

    private final Predicate<String> acceptedMatcher;

    private final Predicate<String> knownMatcher;

    public PredicateSchemaMatcher(Predicate<String> acceptedMatcher) {
        this(acceptedMatcher, s -> false);
    }

    public PredicateSchemaMatcher(Predicate<String> acceptedMatcher, Predicate<String> knownMatcher) {
        this.acceptedMatcher = requireNonNull(acceptedMatcher);
        this.knownMatcher = requireNonNull(knownMatcher);
    }

    @Override
    public SchemaClassification matches(String name) {
        if (acceptedMatcher.test(name)) {
            return ACCEPTED;
        } else if (knownMatcher.test(name)) {
            return KNOWN;
        } else {
            return UNKNOWN;
        }
    }
}
