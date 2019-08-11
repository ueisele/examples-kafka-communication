package net.uweeisele.examples.kafka.sequence.event;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import static java.lang.System.currentTimeMillis;

@JsonPropertyOrder({ "timestamp", "name" })
public abstract class ClientEvent {

    private final String client;
    private final long timestamp;

    public ClientEvent(String client) {
        this(client, currentTimeMillis());
    }

    public ClientEvent(String client, long timestamp) {
        this.client = client;
        this.timestamp = timestamp;
    }

    @JsonProperty
    public String client() {
        return client;
    }

    @JsonProperty
    public abstract String name();

    @JsonProperty
    public long timestamp() {
        return timestamp;
    }

}
