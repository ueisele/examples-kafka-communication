package net.uweeisele.examples.kafka.serde.avro.deserializers.payload;

import org.apache.kafka.common.header.Headers;

public interface HeadersAware extends PayloadAware {

    Headers headers();

    HeadersAware withHeaders(Headers headers);
}
