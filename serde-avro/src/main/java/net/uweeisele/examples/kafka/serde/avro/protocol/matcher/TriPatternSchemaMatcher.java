package net.uweeisele.examples.kafka.serde.avro.protocol.matcher;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static net.uweeisele.examples.kafka.serde.avro.protocol.matcher.SchemaMatcher.SchemaClassification.*;

public class TriPatternSchemaMatcher implements SchemaMatcher {

    public static final String ACCEPTED_GROUP_NAME = "accepted";

    private final Pattern pattern;

    private final boolean hasAcceptedGroup;

    public TriPatternSchemaMatcher(Pattern pattern) {
        this.pattern = requireNonNull(pattern);
        this.hasAcceptedGroup = hasAcceptedGroup(pattern);
    }

    @Override
    public SchemaClassification matches(String name) {
        Matcher matcher = pattern.matcher(name);
        if(matcher.matches()) {
            if (!hasAcceptedGroup || matchesAcceptedGroup(matcher)) {
                return ACCEPTED;
            }
            return KNOWN;
        }
        return UNKNOWN;
    }

    private boolean matchesAcceptedGroup(Matcher matcher) {
        return matcher.group(ACCEPTED_GROUP_NAME) != null;
    }

    public static boolean hasAcceptedGroup(Pattern pattern) {
        return hasAcceptedGroup(pattern.pattern());
    }

    public static boolean hasAcceptedGroup(String regex) {
        return regex.contains(format("(?<%s>", ACCEPTED_GROUP_NAME));
    }

}
