package net.uweeisele.examples.kafka.serde.avro.function;

import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

@FunctionalInterface
public interface ConfigurableConsumer<T> extends Consumer<T>, Configurable {

    @Override
    default ConfigurableConsumer<T> configure(Properties properties) {
        return this;
    }

    default ConfigurableConsumer<T> andThen(Consumer<? super T> after) {
        requireNonNull(after);
        return new ConfigurableConsumer<>() {
            @Override
            public void accept(T t) {
                ConfigurableConsumer.this.accept(t);
                after.accept(t);
            }
            @Override
            public ConfigurableConsumer<T> configure(Properties properties) {
                ConfigurableConsumer.this.configure(properties);
                if(after instanceof Configurable) {
                    ((Configurable) after).configure(properties);
                }
                return this;
            }
        };
    }

    default <V> ConfigurableConsumer<V> compose(Function<? super V, ? extends T> before) {
        requireNonNull(before);
        return new ConfigurableConsumer<>() {
            @Override
            public void accept(V v) {
                ConfigurableConsumer.this.accept(before.apply(v));
            }
            @Override
            public ConfigurableConsumer<V> configure(Properties properties) {
                if(before instanceof Configurable) {
                    ((Configurable) before).configure(properties);
                }
                ConfigurableConsumer.this.configure(properties);
                return this;
            }
        };
    }

    default <V> ConfigurableConsumer<T> andThen(Consumer<? super V> after, Function<? super T, ? extends V> consumerFunction) {
        requireNonNull(after);
        requireNonNull(consumerFunction);
        return new ConfigurableConsumer<>() {
            @Override
            public void accept(T t) {
                ConfigurableConsumer.this.accept(t);
                after.accept(consumerFunction.apply(t));
            }
            @Override
            public ConfigurableConsumer<T> configure(Properties properties) {
                ConfigurableConsumer.this.configure(properties);
                if(after instanceof Configurable) {
                    ((Configurable) after).configure(properties);
                }
                if(consumerFunction instanceof Configurable) {
                    ((Configurable) consumerFunction).configure(properties);
                }
                return this;
            }
        };
    }

    static <T> ConfigurableConsumer<T> noop() {
        return t -> {};
    }

    static <T> ConfigurableConsumer<T> wrap(Consumer<? super T> consumer) {
        requireNonNull(consumer);
        return new ConfigurableConsumer<>() {
            @Override
            public void accept(T t) {
                consumer.accept(t);
            }
            @Override
            public ConfigurableConsumer<T> configure(Properties properties) {
                if(consumer instanceof Configurable) {
                    ((Configurable) consumer).configure(properties);
                }
                return this;
            }
        };
    }

}
