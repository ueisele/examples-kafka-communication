package net.uweeisele.examples.kafka.sequence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.uweeisele.examples.kafka.sequence.event.ClientEvent;

import java.io.PrintStream;

public class JsonPrintEventHandler implements EventHandler {

    private final ObjectMapper mapper = new ObjectMapper();

    private final PrintStream out;

    public JsonPrintEventHandler() {
        this(System.out);
    }

    public JsonPrintEventHandler(PrintStream out) {
        this.out = out;
    }

    @Override
    public void on(ClientEvent event) {
        try {
            out.println(mapper.writeValueAsString(event));
        } catch (JsonProcessingException e) {
            out.println("Bad data can't be written as json: " + e.getMessage());
        }
    }
}
