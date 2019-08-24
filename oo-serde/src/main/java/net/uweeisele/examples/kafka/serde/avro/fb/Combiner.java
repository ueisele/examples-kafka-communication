package net.uweeisele.examples.kafka.serde.avro.fb;

public interface Combiner<T> {

    T combine(T first, T second);

    static <T> Combiner<T> allowNull(Combiner<T> c) {
        return (first, second) -> {
            if (first == null) {
                return second;
            }
            if (second == null) {
                return first;
            }
            return c.combine(first, second);
        };
    }
}
