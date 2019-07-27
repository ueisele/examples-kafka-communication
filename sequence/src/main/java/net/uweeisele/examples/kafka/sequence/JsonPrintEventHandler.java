package net.uweeisele.examples.kafka.sequence;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import net.uweeisele.examples.kafka.sequence.event.ClientEvent;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.io.PrintStream;

public class JsonPrintEventHandler implements EventHandler {

    private final ObjectMapper mapper = new ObjectMapper();

    private final PrintStream out;

    public JsonPrintEventHandler() {
        this(System.out);
    }

    public JsonPrintEventHandler(PrintStream out) {
        this.out = out;
        addKafkaSerializerModule();
    }

    @Override
    public void on(ClientEvent event) {
        try {
            out.println(mapper.writeValueAsString(event));
        } catch (JsonProcessingException e) {
            out.println("Bad data can't be written as json: " + e.getMessage());
        }
    }

    private void addKafkaSerializerModule() {
        SimpleModule kafka = new SimpleModule();
        kafka.addSerializer(TopicPartition.class, new JsonSerializer<TopicPartition>() {
            @Override
            public void serialize(TopicPartition tp, JsonGenerator gen, SerializerProvider serializers) throws IOException {
                gen.writeStartObject();
                gen.writeObjectField("topic", tp.topic());
                gen.writeObjectField("partition", tp.partition());
                gen.writeEndObject();
            }
        });
        mapper.registerModule(kafka);
    }
}
