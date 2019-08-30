package net.uweeisele.examples.kafka.serde.avro.protocol.avro;

import net.uweeisele.examples.kafka.serde.avro.protocol.Payload;
import net.uweeisele.examples.kafka.serde.avro.protocol.Protocol;
import net.uweeisele.examples.kafka.serde.avro.protocol.ProtocolDecoder;
import net.uweeisele.examples.kafka.serde.avro.protocol.avro.reader.DatumReaderBuilder;
import net.uweeisele.examples.kafka.serde.avro.protocol.avro.schema.SchemaResolver;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;

import java.io.IOException;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

public class AvroProtocolDecoder<T extends GenericContainer> implements ProtocolDecoder<Protocol<Integer, Decoder>, T> {

    private final Properties properties;

    private final SchemaResolver schemaResolver;

    private final DatumReaderBuilder<T> datumReaderBuilder;

    public AvroProtocolDecoder(Properties properties, SchemaResolver schemaResolver, DatumReaderBuilder<T> datumReaderBuilder) {
        this.properties = requireNonNull(properties);
        this.schemaResolver = requireNonNull(schemaResolver);
        this.datumReaderBuilder = requireNonNull(datumReaderBuilder);
    }

    @Override
    public Payload<T> decode(Payload<Protocol<Integer, Decoder>> input) throws IOException {
        return input.withBody(decode(input.get().schema(), input.get().content()));
    }

    private T decode(Integer writerSchemaId, Decoder decoder) throws IOException {
        Schema writerSchema = schemaResolver.getSchemaById(writerSchemaId);
        DatumReader<T> datumReader = datumReaderBuilder.build(writerSchema);
        return datumReader.read(null, decoder);
    }
}
