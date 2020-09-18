package net.uweeisele.examples.kafka.sequence;

import com.fasterxml.jackson.annotation.JsonProperty;
import net.uweeisele.examples.kafka.sequence.event.ClientEvent;
import net.uweeisele.examples.kafka.sequence.event.ShutdownComplete;
import net.uweeisele.examples.kafka.sequence.event.StartupComplete;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

public class Generator implements Runnable, AutoCloseable {

    private final String clientName;

    private final Producer<String, String> producer;

    private final String topic;

    private final Set<String> keys;

    private final long maxMessagesPerKey;

    private final ThroughputThrottler throughputThrottler;

    private Instant createTime;

    private boolean syncProduce;

    private final EventHandler eventHandler;

    // Hook to trigger producing thread to stop sending messages
    private volatile boolean stopProducing = false;

    private long numSent = 0;

    private long numAcked = 0;

    private Instant start;

    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    protected Generator(String clientName, Producer<String, String> producer, String topic, Collection<String> keys, long maxMessagesPerKey, ThroughputThrottler throughputThrottler, Instant createTime, boolean syncProduce, EventHandler eventHandler) {
        this.clientName = clientName;
        this.producer = producer;
        this.topic = topic;
        this.keys = new LinkedHashSet<>(keys);
        this.maxMessagesPerKey = maxMessagesPerKey;
        this.throughputThrottler = throughputThrottler;
        this.createTime = createTime;
        this.syncProduce = syncProduce;
        this.eventHandler = eventHandler;
    }

    public void run() {
        if (stopProducing) {
            return;
        }

        eventHandler.on(new StartupComplete(clientName));
        eventHandler.on(new KeysGenerated(clientName, keys));

        // negative maxMessages (-1) means "infinite"
        long maxMessagesPerKey = (this.maxMessagesPerKey < 0) ? Long.MAX_VALUE : this.maxMessagesPerKey;

        for (long i = 0; i < maxMessagesPerKey; i++) {
            for (String key : keys) {
                if (stopProducing) {
                    break;
                }

                long sendStartMs = System.currentTimeMillis();
                if (start == null) {
                    start = Instant.ofEpochMilli(sendStartMs);
                }

                this.send(key, i);

                if (throughputThrottler.shouldThrottle(i, sendStartMs)) {
                    throughputThrottler.throttle();
                }
            }
        }

        producer.close();
        eventHandler.on(new ProducerStats(clientName, numSent, numAcked, throughputThrottler.getTargetThroughput(), getAvgThroughput()));
        eventHandler.on(new ShutdownComplete(clientName));
        shutdownLatch.countDown();
    }

    public void send(String key, Long value) {
        ProducerRecord<String, String> record;

        Headers headers = new RecordHeaders(List.of(new RecordHeader("producerid", (clientName + "-" + key).getBytes(UTF_8))));
        if (createTime != null) {
            record = new ProducerRecord<>(topic, null, createTime.toEpochMilli(), key, String.valueOf(value), headers);
            createTime = createTime.plusMillis(System.currentTimeMillis() - start.toEpochMilli());
        } else {
            record = new ProducerRecord<>(topic, null, key, String.valueOf(value) ,headers);
        }

        numSent++;
        Future<RecordMetadata> result = null;
        try {
            result = producer.send(record, new EventCallback(key, value));
        } catch (Exception e) {
            synchronized (eventHandler) {
                eventHandler.on(new FailedSend(clientName, key, value, topic, e));
            }
        }
        if (syncProduce && result != null) {
            try {
                result.get();
            } catch (Exception e) {
                // Handled by callback
            }
        }
    }

    public List<String> getKeys() {
        return new ArrayList<>(keys);
    }

    public Instant getStart() {
        return start;
    }

    public long getNumSent() {
        return numSent;
    }

    public long getNumAcked() {
        return numAcked;
    }

    public double getAvgThroughput() {
        long currentMs = System.currentTimeMillis();
        return 1000 * (numAcked / (double) (currentMs - (start != null ? start.toEpochMilli() : currentMs)));
    }

    @Override
    public void close() {
        boolean interrupted = false;
        try {
            stopProducing = true;
            while (true) {
                try {
                    shutdownLatch.await();
                    return;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    private class EventCallback implements Callback {

        private String key;
        private Long value;

        EventCallback(String key, Long value) {
            this.key = key;
            this.value = value;
        }

        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            synchronized (Generator.this.eventHandler) {
                if (e == null) {
                    Generator.this.numAcked++;
                    Generator.this.eventHandler.on(new SuccessfulSend(clientName, this.key, this.value, recordMetadata));
                } else {
                    Generator.this.eventHandler.on(new FailedSend(clientName, this.key, this.value, topic, e));
                }
            }
        }
    }

    public static class Builder {

        private final Producer<String, String> producer;

        private final String topic;

        private String clientName = Generator.class.getSimpleName().toLowerCase();

        // Default is one key
        private Set<String> keys = new LinkedHashSet<>(singletonList(randomUUID().toString()));

        // If maxMessagesPerKey < 0, produce until the process is killed externally
        private long maxMessagesPerKey = -1;

        private ThroughputThrottler throughputThrottler;

        // The create time to set in messages
        private Instant createTime;

        private boolean syncProduce = false;

        private EventHandler eventHandler = new JsonPrintEventHandler();

        public Builder(Producer<String, String> producer, String topic) {
            this.producer = producer;
            this.topic = topic;
        }

        public Builder withClientName(String clientName) {
            this.clientName = requireNonNull(clientName);
            return this;
        }

        // If numKeys <= 0, null is used as key
        public Builder withNumKeys(int numKeys) {
            keys = new LinkedHashSet<>();
            if (numKeys <= 0) {
                keys.add(null);
            } else {
                for (int k = 0; k < numKeys; k++) {
                    keys.add(randomUUID().toString());
                }
            }
            return this;
        }

        public Builder withKeys(Collection<String> keys) {
            this.keys = new LinkedHashSet<>(keys);
            return this;
        }

        public Builder withMaxMessagesPerKey(long maxMessagesPerKey) {
            this.maxMessagesPerKey = maxMessagesPerKey;
            return this;
        }

        public Builder withThroughput(ThroughputThrottler throughputThrottler) {
            this.throughputThrottler = requireNonNull(throughputThrottler);
            return this;
        }

        public Builder withCreateTime(Instant createTime) {
            this.createTime = createTime;
            return this;
        }

        public Builder withSyncProduce(boolean syncProduce) {
            this.syncProduce = syncProduce;
            return this;
        }

        public Builder withEventHandler(EventHandler eventHandler) {
            this.eventHandler = requireNonNull(eventHandler);
            return this;
        }

        public Generator build() {
            return new Generator(clientName, producer, topic, keys, maxMessagesPerKey, throughputThrottler, createTime, syncProduce, eventHandler);
        }
    }

    public static class KeysGenerated extends ClientEvent {

        private Collection<String> keys;

        public KeysGenerated(String clientName, Collection<String> keys) {
            super(clientName);
            this.keys = keys;
        }

        @Override
        public String name() {
            return "producer_keys";
        }

        @JsonProperty
        public Collection<String> keys() {
            return keys;
        }
    }

    public static class SuccessfulSend extends ClientEvent {

        private String key;
        private Long value;
        private RecordMetadata recordMetadata;

        public SuccessfulSend(String clientName, String key, Long value, RecordMetadata recordMetadata) {
            super(clientName);
            assert recordMetadata != null : "Expected non-null recordMetadata object.";
            this.key = key;
            this.value = value;
            this.recordMetadata = recordMetadata;
        }

        @Override
        public String name() {
            return "producer_send_success";
        }

        @JsonProperty
        public String key() {
            return key;
        }

        @JsonProperty
        public Long value() {
            return value;
        }

        @JsonProperty
        public String topic() {
            return recordMetadata.topic();
        }

        @JsonProperty
        public int partition() {
            return recordMetadata.partition();
        }

        @JsonProperty
        public long offset() {
            return recordMetadata.offset();
        }
    }

    public static class FailedSend extends ClientEvent {

        private String topic;
        private String key;
        private Long value;
        private Exception exception;

        public FailedSend(String clientName, String key, Long value, String topic, Exception exception) {
            super(clientName);
            assert exception != null : "Expected non-null exception.";
            this.key = key;
            this.value = value;
            this.topic = topic;
            this.exception = exception;
        }

        @Override
        public String name() {
            return "producer_send_error";
        }

        @JsonProperty
        public String key() {
            return key;
        }

        @JsonProperty
        public Long value() {
            return value;
        }

        @JsonProperty
        public String topic() {
            return topic;
        }

        @JsonProperty
        public String exception() {
            return exception.getClass().toString();
        }

        @JsonProperty
        public String message() {
            return exception.getMessage();
        }
    }

    public static class ProducerStats extends ClientEvent {

        private long sent;
        private long acked;
        private long targetThroughput;
        private double avgThroughput;

        public ProducerStats(String clientName, long sent, long acked, long targetThroughput, double avgThroughput) {
            super(clientName);
            this.sent = sent;
            this.acked = acked;
            this.targetThroughput = targetThroughput;
            this.avgThroughput = avgThroughput;
        }

        @Override
        public String name() {
            return "tool_data";
        }

        @JsonProperty
        public long sent() {
            return this.sent;
        }

        @JsonProperty
        public long acked() {
            return this.acked;
        }

        @JsonProperty("target_throughput")
        public long targetThroughput() {
            return this.targetThroughput;
        }

        @JsonProperty("avg_throughput")
        public double avgThroughput() {
            return this.avgThroughput;
        }
    }
}
