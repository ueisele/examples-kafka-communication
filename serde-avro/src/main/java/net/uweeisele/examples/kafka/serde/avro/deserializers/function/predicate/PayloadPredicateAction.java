package net.uweeisele.examples.kafka.serde.avro.deserializers.function.predicate;

import net.uweeisele.examples.kafka.serde.avro.deserializers.payload.PayloadAware;
import net.uweeisele.examples.kafka.serde.avro.function.ConfigurableConsumer;

import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public class PayloadPredicateAction<S extends PayloadAware, P> implements ConfigurableConsumer<S> {

    private final Function<S, P> extractor;
    private final Predicate<P> predicate;

    public PayloadPredicateAction(Function<S, P> extractor, Predicate<P> predicate) {
        this.extractor = requireNonNull(extractor);
        this.predicate = requireNonNull(predicate);
    }

    @Override
    public void accept(S payload) {
        if (!predicate.test(extractor.apply(payload))) {

        }
    }
}
