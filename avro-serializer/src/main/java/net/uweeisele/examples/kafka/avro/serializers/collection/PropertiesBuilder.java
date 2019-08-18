package net.uweeisele.examples.kafka.avro.serializers.collection;

import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
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

    public PropertiesBuilder withProperty(String key, String value) {
        properties.setProperty(key, value);
        return this;
    }

    public PropertiesBuilder withAll(Map<?, ?> map) {
        return withAllNormalized(map, k -> k, v -> v);
    }

    public PropertiesBuilder withAllNormalized(Map<?, ?> map) {
        return withAllNormalized(map, String::valueOf, String::valueOf);
    }

    public PropertiesBuilder withAllNormalized(Map<?, ?> map, Function<? super Object, ?> keyNormalizer, Function<? super Object, ?> valueNormalizer) {
        map.entrySet().stream()
                .filter(entry -> entry.getKey() != null && entry.getValue() != null)
                .forEach(entry -> properties.put(keyNormalizer.apply(entry.getKey()), valueNormalizer.apply(entry.getValue())));
        return this;
    }

    public  PropertiesBuilder withAll(Properties properties) {
        this.properties.putAll(properties);
        return this;
    }

    public static Properties ofNormalized(Map<?, ?> map) {
        return new PropertiesBuilder()
                .withAllNormalized(map)
                .build();
    }

    public static Properties of(Map<?, ?> map) {
        return new PropertiesBuilder()
                .withAll(map)
                .build();
    }

    public static Properties of(String key, String value) {
        return new PropertiesBuilder()
                .withProperty(key, value)
                .build();
    }
}
