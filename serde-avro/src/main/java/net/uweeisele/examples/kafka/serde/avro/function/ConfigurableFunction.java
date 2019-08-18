package net.uweeisele.examples.kafka.serde.avro.function;

import java.util.Properties;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

@FunctionalInterface
public interface ConfigurableFunction<T, R> extends Function<T, R>, Configurable {

    @Override
    default ConfigurableFunction<T, R> configure(Properties properties) {
        return this;
    }

    default <V> ConfigurableFunction<V, R> compose(Function<? super V, ? extends T> before) {
        requireNonNull(before);
        return new ConfigurableFunction<>() {
            @Override
            public R apply(V t) {
                return ConfigurableFunction.this.apply(before.apply(t));
            }
            @Override
            public ConfigurableFunction<V, R> configure(Properties properties) {
                if (before instanceof Configurable) {
                    ((ConfigurableFunction) before).configure(properties);
                }
                ConfigurableFunction.this.configure(properties);
                return this;
            }
        };
    }

    default <V, U> ConfigurableBiFunction<V, U, R> compose(BiFunction<? super V, ? super U, ? extends T> before) {
        requireNonNull(before);
        return new ConfigurableBiFunction<>() {
            @Override
            public R apply(V t, U u) {
                return ConfigurableFunction.this.apply(before.apply(t, u));
            }
            @Override
            public ConfigurableBiFunction<V, U, R> configure(Properties properties) {
                if (before instanceof Configurable) {
                    ((ConfigurableFunction) before).configure(properties);
                }
                ConfigurableFunction.this.configure(properties);
                return this;
            }
        };
    }

    default <V> ConfigurableFunction<T, V> andThen(Function<? super R, ? extends V> after) {
        requireNonNull(after);
        return new ConfigurableFunction<>() {
            @Override
            public V apply(T t) {
                return after.apply(ConfigurableFunction.this.apply(t));
            }
            @Override
            public ConfigurableFunction<T, V> configure(Properties properties) {
                ConfigurableFunction.this.configure(properties);
                if (after instanceof Configurable) {
                    ((Configurable) after).configure(properties);
                }
                return this;
            }
        };
    }

    default <V, U> ConfigurableBiFunction<T, U, V> andThen(BiFunction<? super R, ? super U, ? extends V> after) {
        requireNonNull(after);
        return new ConfigurableBiFunction<>() {
            @Override
            public V apply(T t, U u) {
                return after.apply(ConfigurableFunction.this.apply(t), u);
            }
            @Override
            public ConfigurableBiFunction<T, U, V> configure(Properties properties) {
                ConfigurableFunction.this.configure(properties);
                if (after instanceof Configurable) {
                    ((Configurable) after).configure(properties);
                }
                return this;
            }
        };
    }

    default ConfigurableFunction<T, R> andPeekBefore(Consumer<? super T> before) {
        requireNonNull(before);
        return new ConfigurableFunction<>() {
            @Override
            public R apply(T t) {
                before.accept(t);
                return ConfigurableFunction.this.apply(t);
            }
            @Override
            public ConfigurableFunction<T, R> configure(Properties properties) {
                if (before instanceof Configurable) {
                    ((Configurable) before).configure(properties);
                }
                ConfigurableFunction.this.configure(properties);
                return this;
            }
        };
    }

    default ConfigurableFunction<T, R> andPeekAfter(Consumer<? super R> after) {
        requireNonNull(after);
        return new ConfigurableFunction<>() {
            @Override
            public R apply(T t) {
                R result = ConfigurableFunction.this.apply(t);
                after.accept(result);
                return result;
            }
            @Override
            public ConfigurableFunction<T, R> configure(Properties properties) {
                ConfigurableFunction.this.configure(properties);
                if (after instanceof Configurable) {
                    ((Configurable) after).configure(properties);
                }
                return this;
            }
        };
    }

    default ConfigurableConsumer<T> andConsume(Consumer<? super R> after) {
        requireNonNull(after);
        return new ConfigurableConsumer<>() {
            @Override
            public void accept(T t) {
                after.accept(ConfigurableFunction.this.apply(t));
            }
            @Override
            public ConfigurableConsumer<T> configure(Properties properties) {
                ConfigurableFunction.this.configure(properties);
                if (after instanceof Configurable) {
                    ((Configurable) after).configure(properties);
                }
                return this;
            }
        };
    }

    static <T> ConfigurableFunction<T, T> identity() {
        return t -> t;
    }

    static <T, R> ConfigurableFunction<T, R> wrap(Function<? super T, ? extends R> function) {
        requireNonNull(function);
        return new ConfigurableFunction<>() {
            @Override
            public R apply(T t) {
                return function.apply(t);
            }
            @Override
            public ConfigurableFunction<T, R> configure(Properties properties) {
                if (function instanceof Configurable) {
                    ((Configurable) function).configure(properties);
                }
                return this;
            }
        };
    }
}
