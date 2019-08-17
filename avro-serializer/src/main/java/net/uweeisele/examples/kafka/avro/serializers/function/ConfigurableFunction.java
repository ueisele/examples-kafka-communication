package net.uweeisele.examples.kafka.avro.serializers.function;

import java.util.Objects;
import java.util.Properties;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

@FunctionalInterface
public interface ConfigurableFunction<T, R> extends Function<T, R>, Configurable {

    default <V> ConfigurableFunction<V, R> compose(Function<? super V, ? extends T> before) {
        Objects.requireNonNull(before);
        return new ConfigurableFunction<>() {
            @Override
            public R apply(V t) {
                return ConfigurableFunction.this.apply(before.apply(t));
            }
            @Override
            public void configure(Properties properties) {
                if (before instanceof Configurable) {
                    ((ConfigurableFunction) before).configure(properties);
                }
                ConfigurableFunction.this.configure(properties);
            }
        };
    }

    default <V, U> ConfigurableBiFunction<V, U, R> compose(BiFunction<? super V, ? super U, ? extends T> before) {
        Objects.requireNonNull(before);
        return new ConfigurableBiFunction<>() {
            @Override
            public R apply(V t, U u) {
                return ConfigurableFunction.this.apply(before.apply(t, u));
            }
            @Override
            public void configure(Properties properties) {
                if (before instanceof Configurable) {
                    ((ConfigurableFunction) before).configure(properties);
                }
                ConfigurableFunction.this.configure(properties);
            }
        };
    }

    default <V> ConfigurableFunction<T, V> andThen(Function<? super R, ? extends V> after) {
        Objects.requireNonNull(after);
        return new ConfigurableFunction<>() {
            @Override
            public V apply(T t) {
                return after.apply(ConfigurableFunction.this.apply(t));
            }
            @Override
            public void configure(Properties properties) {
                ConfigurableFunction.this.configure(properties);
                if (after instanceof Configurable) {
                    ((Configurable) after).configure(properties);
                }
            }
        };
    }

    default <V, U> ConfigurableBiFunction<T, U, V> andThen(BiFunction<? super R, ? super U, ? extends V> after) {
        Objects.requireNonNull(after);
        return new ConfigurableBiFunction<>() {
            @Override
            public V apply(T t, U u) {
                return after.apply(ConfigurableFunction.this.apply(t), u);
            }
            @Override
            public void configure(Properties properties) {
                ConfigurableFunction.this.configure(properties);
                if (after instanceof Configurable) {
                    ((Configurable) after).configure(properties);
                }
            }
        };
    }

    default ConfigurableFunction<T, R> andPeekBefore(Consumer<? super T> before) {
        Objects.requireNonNull(before);
        return new ConfigurableFunction<>() {
            @Override
            public R apply(T t) {
                before.accept(t);
                return ConfigurableFunction.this.apply(t);
            }
            @Override
            public void configure(Properties properties) {
                if (before instanceof Configurable) {
                    ((Configurable) before).configure(properties);
                }
                ConfigurableFunction.this.configure(properties);
            }
        };
    }

    default ConfigurableFunction<T, R> andPeekAfter(Consumer<? super R> after) {
        Objects.requireNonNull(after);
        return new ConfigurableFunction<>() {
            @Override
            public R apply(T t) {
                R result = ConfigurableFunction.this.apply(t);
                after.accept(result);
                return result;
            }
            @Override
            public void configure(Properties properties) {
                ConfigurableFunction.this.configure(properties);
                if (after instanceof Configurable) {
                    ((Configurable) after).configure(properties);
                }
            }
        };
    }

    static <T> ConfigurableFunction<T, T> identity() {
        return t -> t;
    }

}
