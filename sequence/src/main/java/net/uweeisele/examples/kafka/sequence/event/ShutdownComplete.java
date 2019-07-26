package net.uweeisele.examples.kafka.sequence.event;

public class ShutdownComplete extends ClientEvent {
    @Override
    public String name() {
        return "shutdown_complete";
    }
}
