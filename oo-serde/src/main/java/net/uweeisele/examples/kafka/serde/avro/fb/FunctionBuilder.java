package net.uweeisele.examples.kafka.serde.avro.fb;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

@FunctionalInterface
public interface FunctionBuilder<S, R, C> {

    Function<S, R> build(C config);

    default <V> FunctionBuilder<S, V, C> andThen(FunctionBuilder<? super R, ? extends V, ? super C> after) {
        requireNonNull(after);
        return (C c) -> build(c).andThen(after.build(c));
    }

    default <V, X> FunctionBuilder<S, V, C> andThen(FunctionBuilder<? super R, ? extends V, ? super X> after, Function<? super C, ? extends X> configMapper) {
        requireNonNull(after);
        requireNonNull(configMapper);
        return (C c) -> build(c).andThen(after.build(configMapper.apply(c)));
    }

    static <T, C> FunctionBuilder<T, T, C> identityBuilder() {
        return c -> t -> t;
    }

    static <S, R, C> FunctionBuilder<S, R, C> nullBuilder() {
        return c -> s -> null;
    }

}
