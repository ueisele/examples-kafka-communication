package net.uweeisele.examples.kafka.serde.avro.protocol.matcher;

import net.uweeisele.examples.kafka.serde.avro.builder.PropertiesBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.regex.Pattern.compile;

public class PredicateSchemaMatcherBuilder implements SchemaMatcherBuilder {

    public static final String ACCEPTED_EXACT_CONFIG = "schema.matcher.predicate.accepted.exact";
    public static final String ACCEPTED_REGEX_CONFIG = "schema.matcher.predicate.accepted.regex";
    public static final String KNOWN_REGEX_CONFIG = "schema.matcher.predicate.known.regex";

    private Supplier<Properties> propertiesSupplier = Properties::new;

    private List<String> acceptedExact = new ArrayList<>();
    private Predicate<String> accepted = n -> false;
    private Predicate<String> known = n -> false;

    @Override
    public boolean test(Properties properties) {
        Properties actualProperties = combinedProperties(properties);
        return actualProperties.containsKey(ACCEPTED_EXACT_CONFIG) ||
                actualProperties.containsKey(ACCEPTED_REGEX_CONFIG);
    }

    @Override
    public PredicateSchemaMatcher build(Properties properties) {
        evaluateProperties(properties);
        Predicate<String> actualAccepted;
        if (!acceptedExact.isEmpty()) {
            actualAccepted = new ContainsPredicate(acceptedExact).or(accepted);
        } else {
            actualAccepted = accepted;
        }
        return new PredicateSchemaMatcher(actualAccepted, known);
    }

    private Properties combinedProperties(Properties properties) {
        return new PropertiesBuilder()
                .withAll(propertiesSupplier.get())
                .withAll(properties)
                .get();
    }

    private void evaluateProperties(Properties properties) {
        Properties actualProperties = combinedProperties(properties);
        String acceptedExact = actualProperties.getProperty(ACCEPTED_EXACT_CONFIG);
        if (acceptedExact != null) {
           addAllAcceptedExact(asList(acceptedExact.split(",")));
        }
        String acceptedRegex = actualProperties.getProperty(ACCEPTED_REGEX_CONFIG);
        if (acceptedRegex != null) {
            addAcceptedRegex(acceptedRegex);
        }
        String knownRegex = actualProperties.getProperty(KNOWN_REGEX_CONFIG);
        if (knownRegex != null) {
            addKnownRegex(knownRegex);
        }
    }

    public PredicateSchemaMatcherBuilder addAllAcceptedExact(Collection<String> names) {
        acceptedExact.addAll(names);
        return this;
    }

    public PredicateSchemaMatcherBuilder addAcceptedExact(String name) {
        acceptedExact.add(name);
        return this;
    }

    public PredicateSchemaMatcherBuilder addAcceptedRegex(String regex) {
        return addAcceptedPattern(compile(regex));
    }

    public PredicateSchemaMatcherBuilder addAcceptedPattern(Pattern pattern) {
        Predicate<String> predicate = new PatternPredicate(pattern);
        accepted = accepted.or(predicate);
        return this;
    }

    public PredicateSchemaMatcherBuilder addKnownRegex(String regex) {
        return addKnownPattern(compile(regex));
    }

    public PredicateSchemaMatcherBuilder addKnownPattern(Pattern pattern) {
        Predicate<String> predicate = new PatternPredicate(pattern);
        known = known.or(predicate);
        return this;
    }

    public PredicateSchemaMatcherBuilder withProperties(Properties properties) {
        requireNonNull(properties);
        return withPropertiesSupplier(() -> properties);
    }

    public PredicateSchemaMatcherBuilder withPropertiesSupplier(Supplier<Properties> propertiesSupplier) {
        this.propertiesSupplier = requireNonNull(propertiesSupplier);
        return this;
    }
}
