package net.uweeisele.example.kafka.producer;

import net.uweeisele.examples.kafka.transformer.CliPropertiesBuilder;
import net.uweeisele.examples.kafka.transformer.KafkaStreamsRunner;

import static net.uweeisele.examples.kafka.transformer.Transformation.valueTransformation;

public class ProducerStreamsCommand {

    public static void main(String[] args) {
        System.exit(
                new KafkaStreamsRunner(
                    new ProducerStreamsBuilder()
                            .withKeyValueMapper(valueTransformation(new V1CountEventMapper())),
                    new CliPropertiesBuilder(ProducerStreamsCommand.class.getSimpleName(), args)
                ).call()
        );
    }
}
