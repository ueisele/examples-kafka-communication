package net.uweeisele.examples.kafka.transformer;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PropertiesKeyValueMapperBuilder<KS, VS, KD, VD> implements Function<Properties, KeyValueMapperList<KS, VS, KD, VD>> {

    private final Function<Properties, ? extends Collection<String>> mapperToBuildCollectionBuilder;
    private final Map<String, Supplier<? extends KeyValueMapper<KS, VS, KeyValue<? extends KD, ? extends VD>>>> registry;

    public PropertiesKeyValueMapperBuilder(Function<Properties, ? extends Collection<String>> mapperToBuildCollectionBuilder) {
        this(mapperToBuildCollectionBuilder, new HashMap<>());
    }

    public PropertiesKeyValueMapperBuilder(Function<Properties, ? extends Collection<String>> mapperToBuildCollectionBuilder, Map<String, Supplier<? extends KeyValueMapper<KS, VS, KeyValue<? extends KD, ? extends VD>>>> registry) {
        this.mapperToBuildCollectionBuilder = requireNonNull(mapperToBuildCollectionBuilder);
        this.registry = requireNonNull(registry);
    }

    @Override
    public KeyValueMapperList<KS, VS, KD, VD> apply(Properties properties) {
        Collection<String> mappersToBuild = mapperToBuildCollectionBuilder.apply(properties);
        KeyValueMapperList<KS, VS, KD, VD> keyValueMapperList = new KeyValueMapperList<>();
        for (String mapperToBuild : mappersToBuild) {
            if (!registry.containsKey(mapperToBuild)) {
                throw new IllegalArgumentException(format("Mapper with key %s is unknown. Valid values are %s.", mapperToBuild, registry.keySet()));
            }
            keyValueMapperList.add(registry.get(mapperToBuild).get());
        }
        return keyValueMapperList;
    }

    public PropertiesKeyValueMapperBuilder withRegisteredKeyValueMapper(String key, Supplier<? extends KeyValueMapper<KS, VS, KeyValue<? extends KD, ? extends VD>>> mapperSupplier) {
        registry.put(key, mapperSupplier);
        return this;
    }

}
