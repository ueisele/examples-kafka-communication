package net.uweeisele.examples.kafka.sequence.event;

public class ShutdownComplete extends ClientEvent {

    public ShutdownComplete(String client) {
        super(client);
    }

    @Override
    public String name() {
        return "shutdown_complete";
    }
}
