package net.uweeisele.examples.kafka.transformer;

import org.apache.kafka.streams.KafkaStreams;

import java.util.Properties;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class KafkaStreamsCommand implements Runnable, AutoCloseable {

    private final Function<Properties, KafkaStreams> kafkaStreamsBuilder;

    public KafkaStreamsCommand(Function<Properties, KafkaStreams> kafkaStreamsBuilder) {
        this.kafkaStreamsBuilder = requireNonNull(kafkaStreamsBuilder);
    }

    @Override
    public void run() {

    }

    @Override
    public void close() {

    }
}
