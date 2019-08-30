package net.uweeisele.examples.kafka.serde.avro.protocol.matcher;

import net.uweeisele.examples.kafka.serde.avro.builder.PropertiesBuilder;

import java.util.Properties;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static java.util.regex.Pattern.compile;
import static net.uweeisele.examples.kafka.serde.avro.protocol.matcher.SchemaMatcher.SchemaClassification.*;
import static net.uweeisele.examples.kafka.serde.avro.protocol.matcher.TriPatternSchemaMatcher.ACCEPTED_GROUP_NAME;

public class TriPatternSchemaMatcherBuilder implements SchemaMatcherBuilder {

    public static final String REGEX_CONFIG = "schema.matcher.tri.regex";
    public static final String REGEX_DOC = format("Regex against which the full schema name is matched. " +
            "If regex contains a group with name '%s' and the group matched than classification %s is returned if the regex matched against the name, but the group did not, %s is returned, otherwise %s.",
            ACCEPTED_GROUP_NAME, ACCEPTED, KNOWN, UNKNOWN);

    private Supplier<Properties> propertiesSupplier = Properties::new;

    private Pattern pattern;

    @Override
    public boolean test(Properties properties) {
        return getRegex(properties) != null;
    }

    @Override
    public TriPatternSchemaMatcher build(Properties properties) {
        return new TriPatternSchemaMatcher(resolvePattern(properties));
    }

    private Pattern resolvePattern(Properties properties) {
        if (pattern != null) {
            return pattern;
        }
        return ofNullable(getRegex(properties))
                .map(Pattern::compile)
                .orElseThrow(() -> new IllegalArgumentException(format("Missing config parameter '%s'. %s", REGEX_CONFIG, REGEX_DOC)));
    }

    private String getRegex(Properties properties) {
        return new PropertiesBuilder()
                .withAll(propertiesSupplier.get())
                .withAll(properties)
                .get()
            .getProperty(REGEX_CONFIG);
    }

    public TriPatternSchemaMatcherBuilder withRegex(String regex) {
        return withPattern(compile(regex));
    }

    public TriPatternSchemaMatcherBuilder withPattern(Pattern pattern) {
        this.pattern = pattern;
        return this;
    }

    public TriPatternSchemaMatcherBuilder withProperties(Properties properties) {
        requireNonNull(properties);
        return withPropertiesSupplier(() -> properties);
    }

    public TriPatternSchemaMatcherBuilder withPropertiesSupplier(Supplier<Properties> propertiesSupplier) {
        this.propertiesSupplier = requireNonNull(propertiesSupplier);
        return this;
    }

}
