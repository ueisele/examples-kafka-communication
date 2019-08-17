package net.uweeisele.examples.kafka.avro.serializers.function;

import java.util.Objects;
import java.util.Properties;
import java.util.function.Consumer;

@FunctionalInterface
public interface ConfigurableConsumer<T> extends Consumer<T>, Configurable {

    default ConfigurableConsumer<T> andThen(Consumer<? super T> after) {
        Objects.requireNonNull(after);
        return new ConfigurableConsumer<>() {
            @Override
            public void accept(T t) {
                ConfigurableConsumer.this.accept(t);
                after.accept(t);
            }
            @Override
            public void configure(Properties properties) {
                ConfigurableConsumer.this.configure(properties);
                if(after instanceof Configurable) {
                    ((Configurable) after).configure(properties);
                }
            }
        };
    }

    static <T> ConfigurableConsumer<T> noop() {
        return t -> {};
    }

}
