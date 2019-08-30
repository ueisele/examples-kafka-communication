package net.uweeisele.examples.kafka.serde.avro.protocol.matcher;

import java.util.Collection;
import java.util.function.Predicate;

public class ContainsPredicate implements Predicate<String> {

    private final Collection<String> accepted;

    public ContainsPredicate(Collection<String> accepted) {
        this.accepted = accepted;
    }

    @Override
    public boolean test(String name) {
        return accepted.contains(name);
    }
}
