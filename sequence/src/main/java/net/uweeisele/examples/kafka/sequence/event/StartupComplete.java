package net.uweeisele.examples.kafka.sequence.event;

public class StartupComplete extends ClientEvent {

    public StartupComplete(String client) {
        super(client);
    }

    @Override
    public String name() {
        return "startup_complete";
    }
}
