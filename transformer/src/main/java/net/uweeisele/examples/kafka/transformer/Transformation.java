package net.uweeisele.examples.kafka.transformer;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.function.BiFunction;

public class Transformation<KS, VS, KD, VD> implements KeyValueMapper<KS, VS, KeyValue<? extends KD, ? extends VD>> {

    private final BiFunction<? super KS, ? super VS, ? extends KD> keyTransformer;

    private final BiFunction<? super KS, ? super VS, ? extends VD> valueTransformer;

    public Transformation(BiFunction<? super KS, ? super VS, ? extends KD> keyTransformer, BiFunction<? super KS, ? super VS, ? extends VD> valueTransformer) {
        this.keyTransformer = keyTransformer;
        this.valueTransformer = valueTransformer;
    }

    public static <KS, KD, V> Transformation<KS, V, KD, V> keyTransformation(BiFunction<? super KS, ? super V, ? extends KD> keyTransformer) {
        return new Transformation<>(keyTransformer, (ks, v) -> v);
    }

    public static <K, VS, VD> Transformation<K, VS, K, VD> valueTransformation(BiFunction<? super K, ? super VS, ? extends VD> valueTransformer) {
        return new Transformation<>((k, vs) -> k, valueTransformer);
    }

    public static <K, V> Transformation<K, V, K, V> noop() {
        return new Transformation<>((ks, vs) -> ks, (ks, vs) -> vs);
    }

    public BiFunction<? super KS, ? super VS, ? extends KD> keyTransformer() {
        return keyTransformer;
    }

    public BiFunction<? super KS, ? super VS, ? extends VD> valueTransformer() {
        return valueTransformer;
    }

    @Override
    public KeyValue<? extends KD, ? extends VD> apply(KS key, VS value) {
        return new KeyValue<>(keyTransformer.apply(key, value), valueTransformer.apply(key, value));
    }
}
