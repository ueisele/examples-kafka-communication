package net.uweeisele.examples.kafka.sequence;

import net.uweeisele.examples.kafka.sequence.event.ClientEvent;

public interface EventHandler {

    void on(ClientEvent event);
}
