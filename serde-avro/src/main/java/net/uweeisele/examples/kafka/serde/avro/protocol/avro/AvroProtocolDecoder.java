package net.uweeisele.examples.kafka.serde.avro.protocol.avro;

import net.uweeisele.examples.kafka.serde.avro.protocol.Payload;
import net.uweeisele.examples.kafka.serde.avro.protocol.Protocol;
import net.uweeisele.examples.kafka.serde.avro.protocol.ProtocolDecoder;
import net.uweeisele.examples.kafka.serde.avro.protocol.avro.reader.DatumReaderBuilder;
import net.uweeisele.examples.kafka.serde.avro.protocol.avro.reader.SchemaAwareDatumReader;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;

import java.io.IOException;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

public class AvroProtocolDecoder<T> implements ProtocolDecoder<Protocol<Schema, Decoder>, T> {

    private final Properties properties;

    private final DatumReaderBuilder<T> datumReaderBuilder;

    public AvroProtocolDecoder(Properties properties, DatumReaderBuilder<T> datumReaderBuilder) {
        this.properties = requireNonNull(properties);
        this.datumReaderBuilder = requireNonNull(datumReaderBuilder);
    }

    @Override
    public Payload<T> decode(Payload<Protocol<Schema, Decoder>> input) throws IOException {
        return input.withBody(decode(input.get().schema(), input.get().content()));
    }

    private T decode(Schema writerSchema, Decoder decoder) throws IOException {
        SchemaAwareDatumReader<T> datumReader = datumReaderBuilder.build(writerSchema);
        return datumReader.read(null, decoder);
    }
}
