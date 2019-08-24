package net.uweeisele.examples.kafka.serde.avro.deserializers.payload;

public interface TopicAware extends PayloadAware {

    String topic();

    TopicAware withTopic(String topic);

    boolean isKey();

    TopicAware withIsKey(boolean isKey);
}
