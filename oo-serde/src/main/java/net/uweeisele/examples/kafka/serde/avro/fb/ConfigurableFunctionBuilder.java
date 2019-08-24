package net.uweeisele.examples.kafka.serde.avro.fb;

import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static net.uweeisele.examples.kafka.serde.avro.fb.Combiner.allowNull;

public class ConfigurableFunctionBuilder<S, R, C> implements FunctionBuilder<S, R, C> {

    private final FunctionBuilder<S, R, C> internalBuilder;
    private final Combiner<C> combiner;

    private Supplier<C> configSupplier = () -> null;

    public ConfigurableFunctionBuilder(FunctionBuilder<S, R, C> internalBuilder, Combiner<C> combiner) {
        this.internalBuilder = requireNonNull(internalBuilder);
        this.combiner = requireNonNull(combiner);
    }

    @Override
    public Function<S, R> build(C config) {
        return internalBuilder.build(combiner.combine(configSupplier.get(), config));
    }

    public ConfigurableFunctionBuilder<S, R, C> withConfig(C config) {
        return withConfigSupplier(() -> config);
    }

    public ConfigurableFunctionBuilder<S, R, C> withConfigSupplier(Supplier<C> configSupplier) {
        this.configSupplier = requireNonNull(configSupplier);
        return this;
    }

    public static <S, R, C extends Combinable<C>> ConfigurableFunctionBuilder<S, R, C> wrap(FunctionBuilder<S, R, C> internalBuilder) {
        return new ConfigurableFunctionBuilder<>(internalBuilder, allowNull(Combinable::combine));
    }

    public static <T, C extends Combinable<C>> ConfigurableFunctionBuilder<T, T, C> identityBuilder() {
        return identityBuilder(allowNull(Combinable::combine));
    }

    public static <T, C> ConfigurableFunctionBuilder<T, T, C> identityBuilder(Combiner<C> combiner) {
        return new ConfigurableFunctionBuilder<>(c -> t -> t, combiner);
    }
}
