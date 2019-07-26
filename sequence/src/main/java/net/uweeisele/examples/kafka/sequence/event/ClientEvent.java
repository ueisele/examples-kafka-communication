package net.uweeisele.examples.kafka.sequence.event;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({ "timestamp", "name" })
public abstract class ClientEvent {

    private final long timestamp = System.currentTimeMillis();

    @JsonProperty
    public abstract String name();

    @JsonProperty
    public long timestamp() {
        return timestamp;
    }

}
