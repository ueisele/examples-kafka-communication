package net.uweeisele.examples.kafka.serde.avro.protocol.avro;

import net.uweeisele.examples.kafka.serde.avro.protocol.Payload;
import net.uweeisele.examples.kafka.serde.avro.protocol.Protocol;
import net.uweeisele.examples.kafka.serde.avro.protocol.ProtocolFilter;
import net.uweeisele.examples.kafka.serde.avro.protocol.exception.UnexpectedDataException;
import net.uweeisele.examples.kafka.serde.avro.protocol.exception.UnsupportedDataException;
import net.uweeisele.examples.kafka.serde.avro.protocol.matcher.SchemaMatcher;
import net.uweeisele.examples.kafka.serde.avro.protocol.matcher.SchemaMatcher.SchemaClassification;
import org.apache.avro.Schema;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AvroSchemaMatcherProtocolFilter<C> implements ProtocolFilter<Protocol<Schema, C>> {

    private final SchemaMatcher schemaMatcher;

    public AvroSchemaMatcherProtocolFilter(SchemaMatcher schemaMatcher) {
        this.schemaMatcher = requireNonNull(schemaMatcher);
    }

    @Override
    public Payload<Protocol<Schema, C>> filter(Payload<Protocol<Schema, C>> input) {
        Schema schema = input.get().schema();
        SchemaClassification classification = schemaMatcher.matches(schema.getFullName());
        switch (classification) {
            case UNKNOWN:
                throw new UnexpectedDataException(format("Avro data, serialized with schema %s is unexpected.", schema.getFullName()), input);
            case KNOWN:
                throw new UnsupportedDataException(format("Avro data, serialized with schema %s is expected, but unsupported.", schema.getFullName()), input);
            case ACCEPTED:
            default:
        }
        return input;
    }

}
