package net.uweeisele.examples.kafka.avro.serializers.function;

import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

@FunctionalInterface
public interface ConfigurableBiFunction<T, U, R> extends BiFunction<T, U, R>, Configurable {

    default <V> ConfigurableBiFunction<T, U, V> andThen(Function<? super R, ? extends V> after) {
        requireNonNull(after);
        return new ConfigurableBiFunction<>() {
            @Override
            public V apply(T t, U u) {
                return after.apply(ConfigurableBiFunction.this.apply(t, u));
            }
            @Override
            public void configure(Properties properties) {
                ConfigurableBiFunction.this.configure(properties);
                if (after instanceof Configurable) {
                    ((Configurable) after).configure(properties);
                }
            }
        };
    }

    default ConfigurableBiFunction<T, U, R> andPeekBefore(Consumer<? super T> before) {
        return andPeekBefore(ConfigurableBiConsumer.<T, U>noop().andThen(before));
    }

    default <V> ConfigurableBiFunction<T, U, R> andPeekBefore(Consumer<? super V> before, BiFunction<? super T, ? super U, ? extends V> consumerFunction) {
        return andPeekBefore(ConfigurableBiConsumer.<T, U>noop().andThen(before, consumerFunction));
    }

    default ConfigurableBiFunction<T, U, R> andPeekBefore(BiConsumer<? super T, ? super U> before) {
        requireNonNull(before);
        return new ConfigurableBiFunction<>() {
            @Override
            public R apply(T t, U u) {
                before.accept(t, u);
                return ConfigurableBiFunction.this.apply(t, u);
            }
            @Override
            public void configure(Properties properties) {
                ConfigurableBiFunction.this.configure(properties);
                if (before instanceof Configurable) {
                    ((Configurable) before).configure(properties);
                }
            }
        };
    }

    default ConfigurableBiFunction<T, U, R> andPeekAfter(Consumer<? super R> after) {
        requireNonNull(after);
        return new ConfigurableBiFunction<>() {
            @Override
            public R apply(T t, U u) {
                R result = ConfigurableBiFunction.this.apply(t, u);
                after.accept(result);
                return result;
            }
            @Override
            public void configure(Properties properties) {
                ConfigurableBiFunction.this.configure(properties);
                if (after instanceof Configurable) {
                    ((Configurable) after).configure(properties);
                }
            }
        };
    }

    default ConfigurableBiConsumer<T, U> andConsume(Consumer<? super R> after) {
        requireNonNull(after);
        return new ConfigurableBiConsumer<>() {
            @Override
            public void accept(T t, U u) {
                after.accept(ConfigurableBiFunction.this.apply(t, u));
            }
            @Override
            public void configure(Properties properties) {
                ConfigurableBiFunction.this.configure(properties);
                if (after instanceof Configurable) {
                    ((Configurable) after).configure(properties);
                }
            }
        };
    }

    static <T, U, R> ConfigurableBiFunction<T, U, R> wrap(BiFunction<? super T, ? super U, ? extends R> biFunction) {
        requireNonNull(biFunction);
        return new ConfigurableBiFunction<>() {
            @Override
            public R apply(T t, U u) {
                return biFunction.apply(t, u);
            }
            @Override
            public void configure(Properties properties) {
                if (biFunction instanceof Configurable) {
                    ((Configurable) biFunction).configure(properties);
                }
            }
        };
    }

}
