package net.uweeisele.examples.kafka.serde.avro.deserializers.payload;

import java.util.Map;
import java.util.Properties;

public interface PayloadAware {

    Properties configs();

    PayloadAware setConfigs(Properties configs);

    Properties attributes();

    PayloadAware setAttributes(Properties attributes);

    PayloadAware withAttributes(Properties attributes);

    PayloadAware withAttribute(String key, String value);

    Map<String, String> printablePayload();

}
