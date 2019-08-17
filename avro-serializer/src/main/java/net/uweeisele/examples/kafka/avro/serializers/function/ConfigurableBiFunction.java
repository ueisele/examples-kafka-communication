package net.uweeisele.examples.kafka.avro.serializers.function;

import java.util.Objects;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

@FunctionalInterface
public interface ConfigurableBiFunction<T, U, R> extends BiFunction<T, U, R>, Configurable {

    default <V> ConfigurableBiFunction<T, U, V> andThen(Function<? super R, ? extends V> after) {
        Objects.requireNonNull(after);
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
        Objects.requireNonNull(before);
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
        Objects.requireNonNull(after);
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

}
