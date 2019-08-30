package net.uweeisele.examples.kafka.serde.avro.protocol.matcher;

import net.uweeisele.examples.kafka.serde.avro.protocol.matcher.SchemaMatcher.SchemaClassification;
import org.junit.jupiter.api.Test;

import java.util.regex.Pattern;

import static java.util.regex.Pattern.compile;
import static net.uweeisele.examples.kafka.serde.avro.protocol.matcher.SchemaMatcher.SchemaClassification.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TriPatternSchemaMatcherTest {

    @Test
    void testAcceptsWithGroup() {
        assertClassification(compile("^ue\\.avro\\.(?:(?<accepted>v1)|(?:v\\d+))\\.MyEvent$"),
                "ue.avro.v1.MyEvent",
                ACCEPTED);
    }

    @Test
    void testKnownWithGroup() {
        assertClassification(compile("^ue\\.avro\\.(?:(?<accepted>v1)|(?:v\\d+))\\.MyEvent$"),
                "ue.avro.v2.MyEvent",
                KNOWN);
    }

    @Test
    void testUnknownWithGroup() {
        assertClassification(compile("^ue\\.avro\\.(?:(?<accepted>v1)|(?:v\\d+))\\.MyEvent$"),
                "ue.avro.v1.YourEvent",
                UNKNOWN);
    }

    @Test
    void testAcceptsWithoutGroupForFixedVersion() {
        assertClassification(compile("^ue\\.avro\\.v1\\.MyEvent$"),
                "ue.avro.v1.MyEvent",
                ACCEPTED);
    }

    @Test
    void testAcceptsWithoutGroupForVariableVersion() {
        assertClassification(compile("^ue\\.avro\\.(?:v\\d+)\\.MyEvent$"),
                "ue.avro.v2.MyEvent",
                ACCEPTED);
    }

    @Test
    void testUnknownWithoutGroup() {
        assertClassification(compile("^ue\\.avro\\.(?:v\\d+)\\.MyEvent$"),
                "ue.avro.v1.YourEvent",
                UNKNOWN);
    }

    void assertClassification(Pattern pattern, String input, SchemaClassification expectedClassification) {
        TriPatternSchemaMatcher schemaMatcher = new TriPatternSchemaMatcher(pattern);
        assertEquals(schemaMatcher.matches(input), expectedClassification);
    }
}