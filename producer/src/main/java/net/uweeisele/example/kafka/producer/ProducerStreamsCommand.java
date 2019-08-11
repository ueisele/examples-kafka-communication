package net.uweeisele.example.kafka.producer;

import net.uweeisele.examples.kafka.transformer.CliPropertiesBuilder;
import net.uweeisele.examples.kafka.transformer.KafkaStreamsRunner;

public class ProducerStreamsCommand {

    public static void main(String[] args) {
        System.exit(
                new KafkaStreamsRunner(
                    new ProducerStreamsBuilder()
                            .withRegisteredMapper("v1", V1CountEventMapper::new)
                            .withRegisteredMapper("v2", V2CountEventMapper::new)
                            .withDefaultMappers("v1"),
                    new CliPropertiesBuilder(ProducerStreamsCommand.class.getSimpleName(), args)
                ).call()
        );
    }
}
