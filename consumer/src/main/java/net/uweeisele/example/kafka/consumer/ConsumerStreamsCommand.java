package net.uweeisele.example.kafka.consumer;

import net.uweeisele.examples.kafka.avro.v1.CountEvent;
import net.uweeisele.examples.kafka.transformer.CliPropertiesBuilder;
import net.uweeisele.examples.kafka.transformer.KafkaStreamsRunner;

import static net.uweeisele.examples.kafka.transformer.AvroSerdeBuilder.specificAvroSerdeBuilder;
import static net.uweeisele.examples.kafka.transformer.Transformation.valueTransformation;

public class ConsumerStreamsCommand {

    public static void main(String[] args) {
        System.exit(
                new KafkaStreamsRunner(
                    new ConsumerStreamsBuilder<CountEvent>(specificAvroSerdeBuilder())
                            .withKeyValueMapper(valueTransformation(new V1CountEventMapper())),
                    new CliPropertiesBuilder(ConsumerStreamsCommand.class.getSimpleName(), args)
                ).call()
        );
    }
}
