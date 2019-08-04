package net.uweeisele.examples.kafka.transformer;

import java.util.Properties;
import java.util.function.Supplier;

public class PropertiesBuilder implements Supplier<Properties> {

    private final Properties properties = new Properties();

    @Override
    public Properties get() {
        return build();
    }

    public Properties build() {
        return properties;
    }

    public PropertiesBuilder addProperty(String key, String value) {
        properties.setProperty(key, value);
        return this;
    }

    public PropertiesBuilder addAllProperties(Properties properties) {
        this.properties.putAll(properties);
        return this;
    }
}
