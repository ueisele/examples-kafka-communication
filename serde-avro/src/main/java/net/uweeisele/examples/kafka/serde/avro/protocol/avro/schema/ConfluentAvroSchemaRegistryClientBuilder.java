package net.uweeisele.examples.kafka.serde.avro.protocol.avro.schema;

import io.confluent.common.config.AbstractConfig;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.*;
import static java.util.Objects.requireNonNull;
import static net.uweeisele.examples.kafka.serde.avro.builder.PropertiesBuilder.ofNormalized;

public class ConfluentAvroSchemaRegistryClientBuilder implements Function<Properties, AvroSchemaRegistryClient>, Supplier<AvroSchemaRegistryClient> {

    private Properties internalProperties = new Properties();
    private Supplier<Properties> propertiesSupplier = Properties::new;

    @Override
    public AvroSchemaRegistryClient get() {
        return build();
    }

    public AvroSchemaRegistryClient build() {
        return build(new Properties());
    }

    public AvroSchemaRegistryClient build(AbstractConfig config) {
        return build(config.originalsWithPrefix(""));
    }

    public AvroSchemaRegistryClient build(Map<String, ?> config) {
        return build(ofNormalized(config));
    }

    @Override
    public AvroSchemaRegistryClient apply(Properties properties) {
        return build(properties);
    }

    public AvroSchemaRegistryClient build(Properties properties) {
        Properties actualProperties = buildActualProperties(properties);
        return new ConfluentAvroSchemaRegistryClient(buildClient(new AbstractKafkaAvroSerDeConfig(baseConfigDef(), toMap(actualProperties))));
    }

    public Properties buildActualProperties(Properties properties) {
        Properties actualProperties = new Properties();
        actualProperties.putAll(internalProperties);
        actualProperties.putAll(propertiesSupplier.get());
        actualProperties.putAll(properties);
        return actualProperties;
    }

    private SchemaRegistryClient buildClient(AbstractKafkaAvroSerDeConfig config) {
        List<String> urls = config.getSchemaRegistryUrls();
        int maxSchemaObject = config.getMaxSchemasPerSubject();
        Map<String, Object> originals = config.originalsWithPrefix("");
        return new CachedSchemaRegistryClient(urls, maxSchemaObject, originals, config.requestHeaders());
    }

    public ConfluentAvroSchemaRegistryClientBuilder withSchemaRegistryUrls(String schemaRegistryUrls) {
        this.internalProperties.setProperty(SCHEMA_REGISTRY_URL_CONFIG, requireNonNull(schemaRegistryUrls));
        return this;
    }

    public String getSchemaRegistryUrls(Properties properties) {
        return buildActualProperties(properties).getProperty(SCHEMA_REGISTRY_URL_CONFIG);
    }

    public ConfluentAvroSchemaRegistryClientBuilder withMaxSchemasPerSubject(int maxSchemasPerSubject) {
        this.internalProperties.setProperty(MAX_SCHEMAS_PER_SUBJECT_CONFIG, String.valueOf(maxSchemasPerSubject));
        return this;
    }

    public ConfluentAvroSchemaRegistryClientBuilder withConfig(AbstractConfig config) {
        return withConfig(config.originalsWithPrefix(""));
    }

    public ConfluentAvroSchemaRegistryClientBuilder withConfig(Map<String, ?> config) {
        return withProperties(ofNormalized(config));
    }

    public ConfluentAvroSchemaRegistryClientBuilder withProperties(Properties properties) {
        requireNonNull(properties);
        return withPropertiesSupplier(() -> properties);
    }

    public ConfluentAvroSchemaRegistryClientBuilder withPropertiesSupplier(Supplier<Properties> propertiesSupplier) {
        this.propertiesSupplier = requireNonNull(propertiesSupplier);
        return this;
    }

    private static Map<String, Object> toMap(Properties properties) {
        return properties.entrySet().stream()
                .map(entry -> new AbstractMap.SimpleEntry<>(String.valueOf(entry.getKey()), entry.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

}
