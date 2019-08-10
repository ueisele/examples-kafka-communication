package net.uweeisele.examples.kafka.transformer;

import net.sourceforge.argparse4j.ArgumentParserBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.*;
import org.apache.commons.lang3.tuple.Pair;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static net.sourceforge.argparse4j.impl.Arguments.store;

public class CliPropertiesBuilder implements Supplier<Properties> {

    private static final String DEFAULT_NAME = "cli";

    private static final String KEY_PROPERTIES_FILE = "properties-file";
    private static final String KEY_PROPERTIES = "property";

    private final String[] args;

    private final ArgumentParser parser;

    public CliPropertiesBuilder(String[] args) {
        this(DEFAULT_NAME, args);
    }

    public CliPropertiesBuilder(String name, String[] args) {
        this(name, null, args);
    }

    public CliPropertiesBuilder(String name, String description, String[] args) {
        this(parserBuilder(name), description, args);
    }

    CliPropertiesBuilder(ArgumentParserBuilder parserBuilder, String description, String[] args) {
        this(initParser(parserBuilder, description), args);
    }

    CliPropertiesBuilder(ArgumentParser parser, String[] args) {
        this.parser = parser;
        this.args = args;
    }

    @Override
    public Properties get() {
        return build();
    }

    public Properties build() {
        Map<String, Object> attrs = new HashMap<>();
        try {
            parser.parseArgs(args, attrs);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            parser.printHelp();
            throw new IllegalArgumentException(e.getMessage(), e);
        }
        Properties properties = new Properties();
        properties.putAll(getOrDefault(KEY_PROPERTIES_FILE, new Properties(), Properties.class, attrs));
        properties.putAll(getOrDefault(KEY_PROPERTIES, new Properties(), Properties.class, attrs));
        return properties;
    }

    private static ArgumentParserBuilder parserBuilder(String name) {
        return ArgumentParsers
                .newFor(name)
                .addHelp(true);
    }

    private static ArgumentParser initParser(ArgumentParserBuilder parserBuilder, String description) {
        ArgumentParser parser = parserBuilder.build();
        Optional.ofNullable(description).ifPresent(parser::description);
        parser.addArgument("--property")
                .action(new AppendPropertiesArgumentAction())
                .required(false)
                .type(new PairArgumentType())
                .dest(KEY_PROPERTIES)
                .metavar("KEY=VALUE")
                .help("Config property. Properties given by this way have precedence over properties given by properties file.");
        parser.addArgument("--properties-file")
                .action(store())
                .required(false)
                .type(new PropertiesFileArgumentType())
                .dest(KEY_PROPERTIES_FILE)
                .metavar("PROPERTIES_FILE")
                .help("Config properties file.");
        return parser;
    }

    public static <T> T getOrDefault(String key, T defaultValue, Class<T> type, Map<String, Object> attrs) {
        Object value = attrs.getOrDefault(key, defaultValue);
        if (value == null || type.isAssignableFrom(value.getClass())) {
            return (T) value;
        }
        throw new IllegalArgumentException(format("Value of key %s has type %s, but expected %s.", key, value.getClass(), type));
    }

    private static class AppendPropertiesArgumentAction implements ArgumentAction {

        @Override
        public void run(ArgumentParser parser, Argument arg,
                        Map<String, Object> attrs, String flag, Object value)
                throws ArgumentParserException {
            if (value == null) {
                throw new ArgumentParserException(format("%s cannot handle null values!", this.getClass()), parser, arg);
            }
            if (!(value instanceof Map.Entry)) {
                throw new ArgumentParserException(format("%s cannot handle values of type %s. Expects values of type %s.", this.getClass(), value.getClass(), Map.Entry.class), parser, arg);
            }
            Object obj = attrs.computeIfAbsent(arg.getDest(), key -> new Properties());
            if (obj instanceof Properties) {
                Map.Entry<?, ?> entry = (Map.Entry<?, ?>) value;
                ((Properties) obj).setProperty(asStringOrNull(entry.getKey()), asStringOrNull(entry.getValue()));
            }
        }

        @Override
        public boolean consumeArgument() {
            return true;
        }

        @Override
        public void onAttach(Argument arg) {
        }

        private String asStringOrNull(Object value) {
            return value != null ? String.valueOf(value) : null;
        }
    }

    private static class PairArgumentType implements ArgumentType<Pair<String, String>> {

        private final Pattern PATTERN = Pattern.compile("([^=]+)=([^=]+)");

        @Override
        public Pair<String, String> convert(ArgumentParser parser, Argument arg, String value) throws ArgumentParserException {
            Matcher matcher = PATTERN.matcher(value);
            if (matcher.matches()) {
               return Pair.of(matcher.group(1), matcher.group(2));
            }
            throw new ArgumentParserException(format("Invalid property format provided. Expected format witch matches pattern %s.", PATTERN.pattern()), parser, arg);
        }
    }


    private static class PropertiesFileArgumentType implements ArgumentType<Properties> {

        @Override
        public Properties convert(ArgumentParser parser, Argument arg, String value) throws ArgumentParserException {
            Properties properties = new Properties();
            try(InputStream in = new FileInputStream(value)) {
                properties.load(in);
            } catch (FileNotFoundException e) {
                throw new ArgumentParserException(format("Properties file %s does not exist.", value), e, parser, arg);
            } catch (IOException e) {
                throw new ArgumentParserException(format("Properties file %s could not be read.", value), e, parser, arg);
            }
            return properties;
        }
    }

}
