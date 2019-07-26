package net.uweeisele.examples.kafka.sequence.event;

public class StartupComplete extends ClientEvent {
    @Override
    public String name() {
        return "startup_complete";
    }
}
