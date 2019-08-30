package net.uweeisele.examples.kafka.serde.avro.protocol.avro.reader;

import org.apache.avro.Schema;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class SchemaPair {

    private static final int NO_HASHCODE = Integer.MIN_VALUE;

    private final Schema writerSchema;
    private final Schema readerSchema;

    private int hashCode = NO_HASHCODE;

    public SchemaPair(Schema writerSchema, Schema readerSchema) {
        this.writerSchema = requireNonNull(writerSchema);
        this.readerSchema = requireNonNull(readerSchema);
    }

    public Schema writerSchema() {
        return writerSchema;
    }

    public Schema readerSchema() {
        return readerSchema;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaPair that = (SchemaPair) o;
        return writerSchema.equals(that.writerSchema) &&
                readerSchema.equals(that.readerSchema);
    }

    @Override
    public int hashCode() {
        if (hashCode == NO_HASHCODE)
            hashCode = Objects.hash(writerSchema, readerSchema);
        return hashCode;
    }
}
