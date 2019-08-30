package net.uweeisele.examples.kafka.serde.avro.protocol.avro;

import net.uweeisele.examples.kafka.serde.avro.protocol.Payload;
import net.uweeisele.examples.kafka.serde.avro.protocol.Protocol;
import net.uweeisele.examples.kafka.serde.avro.protocol.ProtocolFilter;
import net.uweeisele.examples.kafka.serde.avro.protocol.matcher.SchemaMatcher.SchemaClassification;
import net.uweeisele.examples.kafka.serde.avro.protocol.matcher.TriPatternSchemaMatcher;
import org.apache.avro.Schema;

import static java.util.Objects.requireNonNull;

public class AvroSchemaMatcherProtocolFilter<C> implements ProtocolFilter<Protocol<Schema, C>> {

    private final TriPatternSchemaMatcher schemaMatcher;

    public AvroSchemaMatcherProtocolFilter(TriPatternSchemaMatcher schemaMatcher) {
        this.schemaMatcher = requireNonNull(schemaMatcher);
    }

    @Override
    public Payload<Protocol<Schema, C>> filter(Payload<Protocol<Schema, C>> input) {
        Schema schema = input.get().schema();
        SchemaClassification classification = schemaMatcher.matches(schema.getFullName());
        switch (classification) {
            case UNKNOWN:
                break;
            case KNOWN:
                break;
            case ACCEPTED:
            default:
        }
        return input;
    }

}
