package net.uweeisele.examples.kafka.sequence;

import com.fasterxml.jackson.annotation.JsonProperty;
import net.uweeisele.examples.kafka.sequence.event.ClientEvent;
import net.uweeisele.examples.kafka.sequence.event.ShutdownComplete;
import net.uweeisele.examples.kafka.sequence.event.StartupComplete;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

public class Generator implements Runnable, AutoCloseable {

    private final Producer<String, String> producer;

    private final String topic;

    private final int numKeys;

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

    private List<String> keys = new ArrayList<>();

    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    public Generator(Producer<String, String> producer, String topic, int numKeys, long maxMessagesPerKey, ThroughputThrottler throughputThrottler, Instant createTime, boolean syncProduce, EventHandler eventHandler) {
        this.producer = producer;
        this.topic = topic;
        this.numKeys = numKeys;
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

        eventHandler.on(new StartupComplete());

        // negative maxMessages (-1) means "infinite"
        long maxMessagesPerKey = (this.maxMessagesPerKey < 0) ? Long.MAX_VALUE : this.maxMessagesPerKey;

        keys = new ArrayList<>();
        if (numKeys <= 0) {
            keys.add(null);
        } else {
            for (int k = 0; k < numKeys; k++) {
                keys.add(UUID.randomUUID().toString());
            }
            eventHandler.on(new KeysGenerated(keys));
        }

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
        eventHandler.on(new ProducerStats(numSent, numAcked, throughputThrottler.getTargetThroughput(), getAvgThroughput()));
        eventHandler.on(new ShutdownComplete());
        shutdownLatch.countDown();
    }

    public void send(String key, Long value) {
        ProducerRecord<String, String> record;

        if (createTime != null) {
            record = new ProducerRecord<>(topic, null, createTime.toEpochMilli(), key, String.valueOf(value));
            createTime = createTime.plusMillis(System.currentTimeMillis() - start.toEpochMilli());
        } else {
            record = new ProducerRecord<>(topic, key, String.valueOf(value));
        }

        numSent++;
        Future<RecordMetadata> result = null;
        try {
            result = producer.send(record, new EventCallback(key, value));
        } catch (Exception e) {
            synchronized (eventHandler) {
                eventHandler.on(new FailedSend(key, value, topic, e));
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
                    Generator.this.eventHandler.on(new SuccessfulSend(this.key, this.value, recordMetadata));
                } else {
                    Generator.this.eventHandler.on(new FailedSend(this.key, this.value, topic, e));
                }
            }
        }
    }

    public static class Builder {

        private final Producer<String, String> producer;

        private final String topic;

        // If numKeys <= 0, null is used as key
        private int numKeys = 1;

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

        public Builder withNumKeys(int numKeys) {
            this.numKeys = numKeys;
            return this;
        }

        public Builder withMaxMessagesPerKey(long maxMessagesPerKey) {
            this.maxMessagesPerKey = maxMessagesPerKey;
            return this;
        }

        public Builder withThroughput(ThroughputThrottler throughputThrottler) {
            this.throughputThrottler = throughputThrottler;
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
            this.eventHandler = eventHandler;
            return this;
        }

        public Generator build() {
            return new Generator(producer, topic, numKeys, maxMessagesPerKey, throughputThrottler, createTime, syncProduce, eventHandler);
        }
    }

    public static class KeysGenerated extends ClientEvent {

        private List<String> keys;

        public KeysGenerated(List<String> keys) {
            this.keys = keys;
        }

        @Override
        public String name() {
            return "producer_keys";
        }

        @JsonProperty
        public List<String> keys() {
            return keys;
        }
    }

    public static class SuccessfulSend extends ClientEvent {

        private String key;
        private Long value;
        private RecordMetadata recordMetadata;

        public SuccessfulSend(String key, Long value, RecordMetadata recordMetadata) {
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

        public FailedSend(String key, Long value, String topic, Exception exception) {
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

        public ProducerStats(long sent, long acked, long targetThroughput, double avgThroughput) {
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
