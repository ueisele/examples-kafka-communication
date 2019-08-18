package net.uweeisele.examples.kafka.avro.serializers.function;

import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

@FunctionalInterface
public interface ConfigurableBiConsumer<T, U> extends BiConsumer<T, U>, Configurable {

    default ConfigurableBiConsumer<T, U> andThen(BiConsumer<? super T, ? super U> after) {
        requireNonNull(after);
        return new ConfigurableBiConsumer<>() {
            @Override
            public void accept(T t, U u) {
                ConfigurableBiConsumer.this.accept(t, u);
                after.accept(t, u);
            }
            @Override
            public void configure(Properties properties) {
                ConfigurableBiConsumer.this.configure(properties);
                if(after instanceof Configurable) {
                    ((Configurable) after).configure(properties);
                }
            }
        };
    }

    default <V, W> ConfigurableBiConsumer<V, W> compose(Function<? super V, ? extends T> beforeFirst, Function<? super W, ? extends U> beforeSecond) {
        requireNonNull(beforeFirst);
        requireNonNull(beforeSecond);
        return new ConfigurableBiConsumer<>() {
            @Override
            public void accept(V v, W w) {
                ConfigurableBiConsumer.this.accept(beforeFirst.apply(v), beforeSecond.apply(w));
            }
            @Override
            public void configure(Properties properties) {
                ConfigurableBiConsumer.this.configure(properties);
                if(beforeFirst instanceof Configurable) {
                    ((Configurable) beforeFirst).configure(properties);
                }
                if(beforeSecond instanceof Configurable) {
                    ((Configurable) beforeSecond).configure(properties);
                }
            }
        };
    }

    default ConfigurableBiConsumer<T, U> andThen(Consumer<? super T> after) {
        return andThen(after, (t, u) -> t);
    }

    default <V> ConfigurableBiConsumer<T, U> andThen(Consumer<? super V> after, BiFunction<? super T, ? super U, ? extends V> consumerFunction) {
        requireNonNull(after);
        requireNonNull(consumerFunction);
        return new ConfigurableBiConsumer<>() {
            @Override
            public void accept(T t, U u) {
                ConfigurableBiConsumer.this.accept(t, u);
                after.accept(consumerFunction.apply(t, u));
            }
            @Override
            public void configure(Properties properties) {
                ConfigurableBiConsumer.this.configure(properties);
                if(after instanceof Configurable) {
                    ((Configurable) after).configure(properties);
                }
                if(consumerFunction instanceof Configurable) {
                    ((Configurable) consumerFunction).configure(properties);
                }
            }
        };
    }

    static <T, U> ConfigurableBiConsumer<T, U> noop() {
        return (t, u) -> {};
    }

    static <T, U> ConfigurableBiConsumer<T, U> wrap(BiConsumer<? super T, ? super U> biConsumer) {
        requireNonNull(biConsumer);
        return new ConfigurableBiConsumer<T, U>() {
            @Override
            public void accept(T t, U u) {
                biConsumer.accept(t, u);
            }
            @Override
            public void configure(Properties properties) {
                if(biConsumer instanceof Configurable) {
                    ((Configurable) biConsumer).configure(properties);
                }
            }
        };
    }
}
