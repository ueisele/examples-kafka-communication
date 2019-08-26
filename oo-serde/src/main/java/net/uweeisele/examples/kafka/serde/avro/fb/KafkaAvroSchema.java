package net.uweeisele.examples.kafka.serde.avro.fb;

import org.apache.avro.generic.GenericContainer;
import org.apache.avro.util.ByteBufferInputStream;
import org.apache.avro.util.ByteBufferOutputStream;
import org.apache.kafka.common.errors.SerializationException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.function.Function;

public class KafkaAvroSchema<T extends GenericContainer> {

    private final Config config;

    public KafkaAvroSchema(Config config) {
        this.config = config;
    }

    public Payload<byte[]> serialize(Payload<T> payload) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(Integer.MAX_VALUE);
        ObjectOutputStream objOut = new ObjectOutputStream(out);

        objOut.write(0);
        objOut.write(ByteBuffer.allocate(4).putInt(id).array());
        objOut.writeObject(payload.get().getSchema(), payload.get());

        objOut.flush();
        return payload.withBody(out.toByteArray());
    }

    public Payload<T> deserialize(Payload<byte[]> payload) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(payload.get());
        ByteBufferOutputStream bu = new ByteBufferOutputStream();
        byteBuffer.s

        if (byteBuffer.get() != 0)
            throw new SerializationException("");
        int schemaId = byteBuffer.getInt();
        T object = reader.read(schemaResolver.get(schemaId), byteBuffer);

        return payload.withBody(object);
    }
}
