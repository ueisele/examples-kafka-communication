package net.uweeisele.examples.kafka.avro.serializers.builder;

import io.confluent.common.config.AbstractConfig;
import io.confluent.common.config.ConfigException;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.*;
import static java.util.Objects.requireNonNull;

public class SchemaRegistryClientBuilder implements Function<Properties, SchemaRegistryClient>, Supplier<SchemaRegistryClient> {

    private static final String MOCK_URL_PREFIX = "mock://";

    private Properties internalProperties = new Properties();
    private Supplier<Properties> propertiesSupplier = Properties::new;

    @Override
    public SchemaRegistryClient get() {
        return build();
    }

    public SchemaRegistryClient build() {
        return build(new Properties());
    }

    public SchemaRegistryClient build(AbstractConfig config) {
        return build(config.originalsWithPrefix(""));
    }

    public SchemaRegistryClient build(Map<String, ?> config) {
        Properties properties = new Properties();
        properties.putAll(config);
        return build(properties);
    }

    @Override
    public SchemaRegistryClient apply(Properties properties) {
        return build(properties);
    }

    public SchemaRegistryClient build(Properties properties) {
        Properties actualProperties = buildActualProperties(properties);
        return buildClient(new AbstractKafkaAvroSerDeConfig(baseConfigDef(), toMap(actualProperties)));
    }

    public Properties buildActualProperties(Properties properties) {
        Properties actualProperties = new Properties();
        actualProperties.putAll(internalProperties);
        actualProperties.putAll(propertiesSupplier.get());
        actualProperties.putAll(properties);
        return actualProperties;
    }

    public SchemaRegistryClientBuilder withSchemaRegistryUrls(String schemaRegistryUrls) {
        this.internalProperties.setProperty(SCHEMA_REGISTRY_URL_CONFIG, requireNonNull(schemaRegistryUrls));
        return this;
    }

    public String getSchemaRegistryUrls(Properties properties) {
        return buildActualProperties(properties).getProperty(SCHEMA_REGISTRY_URL_CONFIG);
    }

    public SchemaRegistryClientBuilder withMaxSchemasPerSubject(int maxSchemasPerSubject) {
        this.internalProperties.setProperty(MAX_SCHEMAS_PER_SUBJECT_CONFIG, String.valueOf(maxSchemasPerSubject));
        return this;
    }

    public SchemaRegistryClientBuilder withAutoRegisterSchemas(boolean autoRegisterSchemas) {
        this.internalProperties.setProperty(AUTO_REGISTER_SCHEMAS, String.valueOf(autoRegisterSchemas));
        return this;
    }

    public SchemaRegistryClientBuilder withConfig(AbstractConfig config) {
        return withConfig(config.originalsWithPrefix(""));
    }

    public SchemaRegistryClientBuilder withConfig(Map<String, ?> config) {
        Properties properties = new Properties();
        properties.putAll(config);
        return withProperties(properties);
    }

    public SchemaRegistryClientBuilder withProperties(Properties properties) {
        requireNonNull(properties);
        return withPropertiesSupplier(() -> properties);
    }

    public SchemaRegistryClientBuilder withPropertiesSupplier(Supplier<Properties> propertiesSupplier) {
        this.propertiesSupplier = requireNonNull(propertiesSupplier);
        return this;
    }

    private SchemaRegistryClient buildClient(AbstractKafkaAvroSerDeConfig config) {
        List<String> urls = config.getSchemaRegistryUrls();
        int maxSchemaObject = config.getMaxSchemasPerSubject();
        Map<String, Object> originals = config.originalsWithPrefix("");
        String mockScope = validateAndMaybeGetMockScope(urls);
        if (mockScope != null) {
            return MockSchemaRegistry.getClientForScope(mockScope);
        } else {
            return new CachedSchemaRegistryClient(urls, maxSchemaObject, originals, config.requestHeaders());
        }
    }

    private static Map<String, Object> toMap(Properties properties) {
        return properties.entrySet().stream()
                .map(entry -> new AbstractMap.SimpleEntry<>(String.valueOf(entry.getKey()), entry.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static String validateAndMaybeGetMockScope(final List<String> urls) {
        final List<String> mockScopes = new LinkedList<>();
        for (final String url : urls) {
            if (url.startsWith(MOCK_URL_PREFIX)) {
                mockScopes.add(url.substring(MOCK_URL_PREFIX.length()));
            }
        }

        if (mockScopes.isEmpty()) {
            return null;
        } else if (mockScopes.size() > 1) {
            throw new ConfigException(
                    "Only one mock scope is permitted for 'schema.registry.url'. Got: " + urls
            );
        } else if (urls.size() > mockScopes.size()) {
            throw new ConfigException(
                    "Cannot mix mock and real urls for 'schema.registry.url'. Got: " + urls
            );
        } else {
            return mockScopes.get(0);
        }
    }

}
