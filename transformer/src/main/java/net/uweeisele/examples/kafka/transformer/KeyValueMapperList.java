package net.uweeisele.examples.kafka.transformer;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public class KeyValueMapperList<KS, VS, KD, VD> extends ArrayList<KeyValueMapper<? super KS, ? super VS, KeyValue<? extends KD, ? extends VD>>> implements KeyValueMapper<KS, VS, List<? extends KeyValue<? extends KD, ? extends VD>>> {

    public KeyValueMapperList() {
        this(emptyList());
    }

    public KeyValueMapperList(KeyValueMapper<? super KS, ? super VS, KeyValue<? extends KD, ? extends VD>> keyValueMapper) {
        this(singletonList(keyValueMapper));
    }

    public KeyValueMapperList(Collection<KeyValueMapper<? super KS, ? super VS, KeyValue<? extends KD, ? extends VD>>> keyValueMappers) {
        super(keyValueMappers);
    }

    public static <K, V> KeyValueMapperList<K, V, K, V> noop() {
        return new KeyValueMapperList<>(Transformation.noop());
    }

    public static <KS, VS, KD, VD> KeyValueMapperList<KS, VS, KD, VD> empty() {
        return new KeyValueMapperList<>();
    }

    public static <KS, VS, KD, VD> KeyValueMapperList<KS, VS, KD, VD> none() {
        return new KeyValueMapperList<>();
    }

    @Override
    public List<? extends KeyValue<? extends KD, ? extends VD>> apply(KS key, VS value) {
        return this.stream()
                .map(f -> f.apply(key, value))
                .collect(toList());
    }

    public KeyValueMapperList<KS, VS, KD, VD> with(KeyValueMapper<? super KS, ? super VS, KeyValue<? extends KD, ? extends VD>> keyValueMapper) {
        add(keyValueMapper);
        return this;
    }

    public KeyValueMapperList<KS, VS, KD, VD> withAll(Collection<KeyValueMapper<? super KS, ? super VS, KeyValue<? extends KD, ? extends VD>>> keyValueMappers) {
        addAll(keyValueMappers);
        return this;
    }
}
