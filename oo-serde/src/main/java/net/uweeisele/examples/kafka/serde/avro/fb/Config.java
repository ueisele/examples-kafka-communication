package net.uweeisele.examples.kafka.serde.avro.fb;

import java.util.Properties;

import static java.util.Objects.requireNonNull;
import static net.uweeisele.examples.kafka.serde.avro.builder.PropertiesBuilder.of;

public class Config implements Combinable<Config> {

    private final Properties properties;

    private final Type type;

    public Config(Config config) {
        this(config.properties, config.type);
    }

    public Config(Type type) {
        this(new Properties(), type);
    }

    public Config(Properties properties, boolean isKey) {
        this.properties = requireNonNull(properties);
        this.type = Type.isKey(isKey);
    }

    public Config(Properties properties, Type type) {
        this.properties = requireNonNull(properties);
        this.type = requireNonNull(type);
    }

    public Properties properties() {
        return properties;
    }

    public Type type() {
        return type;
    }

    public boolean isKey() {
        return type.isKey();
    }

    @Override
    public Config combine(Config config) {
        if (config == null) {
            return this;
        }
        return new Config(of(properties, config.properties), config.type);
    }

    enum Type {

        KEY(true),
        VALUE(false);

        private final boolean isKey;

        Type(boolean isKey) {
            this.isKey = isKey;
        }

        public boolean isKey() {
            return isKey;
        }

        public static Type isKey(boolean isKey) {
            return isKey ? KEY : VALUE;
        }
    }
}
