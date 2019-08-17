package net.uweeisele.examples.kafka.avro.serializers.function;

import java.util.Objects;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

@FunctionalInterface
public interface ConfigurableBiConsumer<T, U> extends BiConsumer<T, U>, Configurable {

    default ConfigurableBiConsumer<T, U> andThen(BiConsumer<? super T, ? super U> after) {
        Objects.requireNonNull(after);
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

    default ConfigurableBiConsumer<T, U> andThen(Consumer<? super T> after) {
        return andThen(after, (t, u) -> t);
    }

    default <V> ConfigurableBiConsumer<T, U> andThen(Consumer<? super V> after, BiFunction<? super T, ? super U, ? extends V> consumerFunction) {
        Objects.requireNonNull(after);
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
            }
        };
    }

    static <T, U> ConfigurableBiConsumer<T, U> noop() {
        return (t, u) -> {};
    }
}
