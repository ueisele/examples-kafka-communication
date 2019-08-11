package net.uweeisele.examples.kafka.sequence;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import net.uweeisele.examples.kafka.sequence.event.ClientEvent;
import net.uweeisele.examples.kafka.sequence.event.ShutdownComplete;
import net.uweeisele.examples.kafka.sequence.event.StartupComplete;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static java.lang.Long.parseLong;
import static java.util.Collections.emptyList;
import static java.util.Collections.newSetFromMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.*;

public class Verifier implements Runnable, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Verifier.class);

    private final String clientName;

    private final KafkaConsumer<String, String> consumer;

    private final String topic;

    private final boolean useAutoCommit;

    private final boolean useAsyncCommit;

    private final RecordFilter recordFilter;

    private final EventHandler eventHandler;

    private final Map<String, List<String>> valuesByKey = new ConcurrentHashMap<>();

    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    protected Verifier(String clientName, KafkaConsumer<String, String> consumer, String topic, boolean useAutoCommit, boolean useAsyncCommit, RecordFilter recordFilter, EventHandler eventHandler) {
        this.clientName = clientName;
        this.consumer = consumer;
        this.topic = topic;
        this.useAutoCommit = useAutoCommit;
        this.useAsyncCommit = useAsyncCommit;
        this.recordFilter = recordFilter;
        this.eventHandler = eventHandler;
    }

    public boolean isFinished() {
        return recordFilter.isFinished();
    }

    @Override
    public void run() {
        try {
            eventHandler.on(new StartupComplete(clientName));
            consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    eventHandler.on(new PartitionsRevoked(clientName, partitions));
                }
                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    eventHandler.on(new PartitionsAssigned(clientName, partitions));
                }
            });

            while (!isFinished()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                Map<TopicPartition, OffsetAndMetadata> offsets = onRecordsReceived(records);

                if (!useAutoCommit) {
                    if (useAsyncCommit)
                        consumer.commitAsync(offsets, this::onComplete);
                    else
                        commitSync(offsets);
                }
            }
        } catch (WakeupException e) {
            // ignore, we are closing
            log.trace("Caught WakeupException because consumer is shutdown, ignore and terminate.", e);
        } catch (Throwable t) {
            // Log the error so it goes to the service log and not stdout
            log.error("Error during processing, terminating consumer process: ", t);
        } finally {
            consumer.close();
            verificationSummary().values().forEach(eventHandler::on);
            eventHandler.on(new ShutdownComplete(clientName));
            shutdownLatch.countDown();
        }
    }

    private Map<TopicPartition, OffsetAndMetadata> onRecordsReceived(ConsumerRecords<String, String> records) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

        List<RecordSetSummary> summaries = new ArrayList<>();
        long totalRecords = 0;
        long totalAccepted = 0;
        for (TopicPartition tp : records.partitions()) {
            List<ConsumerRecord<String, String>> partitionRecords = records.records(tp);

            if (partitionRecords.isEmpty())
                continue;

            long minOffset = partitionRecords.get(0).offset();
            long maxOffset = minOffset;
            long count = 0;
            long accepted = 0;

            for (ConsumerRecord<String, String> record : partitionRecords) {
              if(isFinished()) {
                  break;
              } else {
                  maxOffset = record.offset();
                  count++;
              }
              if (recordFilter.shouldAccept(record)) {
                  accepted++;
                  eventHandler.on(verify(record));
              }
            }

            offsets.put(tp, new OffsetAndMetadata(maxOffset + 1));
            summaries.add(new RecordSetSummary(tp.topic(), tp.partition(),
                    count, accepted, minOffset, maxOffset));

            totalRecords += count;
            totalAccepted += accepted;

            if (isFinished())
                break;
        }

        eventHandler.on(new RecordsConsumed(clientName, totalRecords, totalAccepted, summaries));
        return offsets;
    }

    private RecordReceived verify(ConsumerRecord<String, String> record) {
        RecordReceived recordReceived;
        if(isInteger(record.value()) && isInSequence(record.key(), parseLong(record.value()))) {
            recordReceived = new ValidRecordReceived(clientName, record);
        } else {
            recordReceived = new UnexpectedRecordReceived(clientName, record);
        }
        valuesByKey.computeIfAbsent(record.key(), key -> new ArrayList<>()).add(record.value());
        return recordReceived;
    }

    public Map<String, RecordVerificationSummary> verificationSummary() {
        Map<String, RecordVerificationSummary> summaries = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : new HashMap<>(valuesByKey).entrySet()) {
            String key = entry.getKey();
            long values = entry.getValue().size();
            long duplicates = entry.getValue().size() - new HashSet<>(entry.getValue()).size();
            long missing = recordFilter.missingRecords(key);
            long invalid = entry.getValue().stream().filter(value -> !isInteger(value)).count();
            boolean outOfSequence = !isInSequence(key);
            summaries.put(key, new RecordVerificationSummary(clientName, key, values, duplicates, missing, invalid, outOfSequence));
        }
        return summaries;
    }

    private boolean isInteger(String value) {
        return value != null && value.matches("\\d+");
    }

    private boolean isInSequence(String key, long value) {
        List<Long> values = valuesByKey.getOrDefault(key, emptyList()).stream()
                .filter(this::isInteger).map(Long::parseLong).collect(toList());

        return (values.isEmpty() && value == 0) ||
                (!values.isEmpty() && values.get(values.size() - 1).equals(value - 1) && value > values.stream().max(Long::compare).orElse(0L));
    }

    private boolean isInSequence(String key) {
        List<Long> values = valuesByKey.getOrDefault(key, emptyList()).stream()
                .filter(this::isInteger).map(Long::parseLong).collect(toList());
        if(!values.isEmpty()) {
            List<Long> expected = LongStream.rangeClosed(values.get(0), values.get(values.size() - 1)).boxed().collect(toList());
            return values.equals(expected);
        }
        return true;
    }

    private void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        try {
            consumer.commitSync(offsets);
            onComplete(offsets, null);
        } catch (WakeupException e) {
            // we only call wakeup() once to close the consumer, so this recursion should be safe
            commitSync(offsets);
            throw e;
        } catch (FencedInstanceIdException e) {
            throw e;
        } catch (Exception e) {
            onComplete(offsets, e);
        }
    }

    private void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        List<CommitData> committedOffsets = new ArrayList<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> offsetEntry : offsets.entrySet()) {
            TopicPartition tp = offsetEntry.getKey();
            committedOffsets.add(new CommitData(tp.topic(), tp.partition(), offsetEntry.getValue().offset()));
        }

        boolean success = true;
        String error = null;
        if (exception != null) {
            success = false;
            error = exception.getMessage();
        }
        eventHandler.on(new OffsetsCommitted(clientName, committedOffsets, error, success));
    }

    @Override
    public void close() {
        boolean interrupted = false;
        try {
            consumer.wakeup();
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

    public static class Builder {

        private final KafkaConsumer<String, String> consumer;

        private final String topic;

        private String clientName = Verifier.class.getSimpleName().toLowerCase();

        private boolean useAutoCommit = false;

        private boolean useAsyncCommit = false;

        private Supplier<ValueFilter> keyFilterSupplier = () -> new DistinctMaxAcceptedValueFilter(1);

        private Supplier<ValueFilter> valueFilterSupplier = () -> new SequenceValueFilter(Long.MAX_VALUE);

        private EventHandler eventHandler = new JsonPrintEventHandler();

        public Builder(KafkaConsumer<String, String> consumer, String topic) {
            this.consumer = consumer;
            this.topic = topic;
        }

        public Verifier build() {
            return new Verifier(clientName, consumer, topic, useAutoCommit, useAsyncCommit, new RecordFilter(keyFilterSupplier, valueFilterSupplier), eventHandler);
        }

        public Builder withClientName(String clientName) {
            this.clientName = requireNonNull(clientName);
            return this;
        }

        public Builder withAutoCommit(boolean useAutoCommit) {
            this.useAutoCommit = useAutoCommit;
            return this;
        }

        public Builder withAsyncCommit(boolean useAsyncCommit) {
            this.useAsyncCommit = useAsyncCommit;
            return this;
        }

        public Builder withKeyFilterSupplier(Supplier<ValueFilter> keyFilterSupplier) {
            this.keyFilterSupplier = requireNonNull(keyFilterSupplier);
            return this;
        }

        public Builder withValueFilterSupplier(Supplier<ValueFilter> valueFilterSupplier) {
            this.valueFilterSupplier = requireNonNull(valueFilterSupplier);
            return this;
        }

        public Builder withEventHandler(EventHandler eventHandler) {
            this.eventHandler = requireNonNull(eventHandler);
            return this;
        }
    }

    public static class RecordFilter {

        private final ValueFilter keyFilter;

        private final Supplier<ValueFilter> valueFilterSupplier;

        private final Map<String, ValueFilter> valueFiltersPerKey;

        public RecordFilter(Supplier<ValueFilter> keyFilterSupplier, Supplier<ValueFilter> valueFilterSupplier) {
            this(keyFilterSupplier.get(), valueFilterSupplier);
        }

        public RecordFilter(ValueFilter keyFilter, Supplier<ValueFilter> valueFilterSupplier) {
            this(keyFilter, valueFilterSupplier, new ConcurrentHashMap<>());
        }

        RecordFilter(ValueFilter keyFilter, Supplier<ValueFilter> valueFilterSupplier, Map<String, ValueFilter> valueFiltersPerKey) {
            this.keyFilter = keyFilter;
            this.valueFilterSupplier = valueFilterSupplier;
            this.valueFiltersPerKey = valueFiltersPerKey;
        }

        public boolean shouldAccept(ConsumerRecord<String, String> record) {
            return keyFilter.shouldAccept(record.key()) &&
                    valueFiltersPerKey.computeIfAbsent(record.key(), key -> valueFilterSupplier.get()).shouldAccept(record.value());
        }

        boolean isFinished() {
            return keyFilter.isFinished() &&
                    valueFiltersPerKey.values().stream().allMatch(ValueFilter::isFinished);
        }

        long consumedKeys() {
            return keyFilter.accepted();
        }

        long missingKeys() {
            return keyFilter.missing();
        }

        long consumedRecords() {
            return valueFiltersPerKey.values().stream().mapToLong(ValueFilter::accepted).sum();
        }

        long missingRecords(String key) {
            return valueFiltersPerKey.getOrDefault(key, valueFilterSupplier.get()).missing();
        }

    }

    public interface ValueFilter {

        boolean shouldAccept(String text);

        long maxAccepted();

        long accepted();

        boolean isFinished();

        long missing();
    }

    public static class DistinctMaxAcceptedValueFilter implements ValueFilter {

        protected final long maxValues;

        protected final Set<String> acceptedValues;

        public DistinctMaxAcceptedValueFilter(long maxValues) {
            this(maxValues, newSetFromMap(new ConcurrentHashMap<>()));
        }

        DistinctMaxAcceptedValueFilter(long maxValues, Set<String> acceptedValues) {
            if(maxValues < 0) {
                throw new IllegalArgumentException("maxValues must be >= 0");
            }
            this.maxValues = maxValues;
            this.acceptedValues = acceptedValues;
        }

        @Override
        public boolean shouldAccept(String value) {
            if (acceptedValues.size() < maxValues) {
                acceptedValues.add(value);
            }
            return acceptedValues.contains(value);
        }

        @Override
        public long maxAccepted() {
            return maxValues;
        }

        @Override
        public long accepted() {
            return acceptedValues.size();
        }

        @Override
        public boolean isFinished() {
            return accepted() >= maxAccepted();
        }

        @Override
        public long missing() {
            return maxAccepted() - accepted();
        }
    }

    public static class FixedValueFilter extends DistinctMaxAcceptedValueFilter {

        protected final Set<String> values;

        public FixedValueFilter(Set<String> values) {
            super(values.size());
            this.values = values;
        }

        @Override
        public boolean shouldAccept(String value) {
            return values.contains(value) && super.shouldAccept(value);
        }

    }

    public static class MaxAcceptedValueFilter implements ValueFilter {

        protected final long maxValues;

        protected final List<String> acceptedValues;

        public MaxAcceptedValueFilter(long maxValues) {
            this(maxValues, new CopyOnWriteArrayList<>());
        }

        MaxAcceptedValueFilter(long maxValues, List<String> acceptedValues) {
            this.maxValues = maxValues;
            this.acceptedValues = acceptedValues;
        }

        @Override
        public boolean shouldAccept(String value) {
            if(!isFinished()) {
                acceptedValues.add(value);
                return true;
            }
            return false;
        }

        @Override
        public long maxAccepted() {
            return maxValues;
        }

        @Override
        public long accepted() {
            return acceptedValues.size();
        }

        @Override
        public boolean isFinished() {
            return accepted() >= maxAccepted();
        }

        @Override
        public long missing() {
            return maxAccepted() - accepted();
        }

    }

    public static class SequenceValueFilter extends MaxAcceptedValueFilter {

        protected final long numbers;

        public SequenceValueFilter(long numbers) {
            super(Long.MAX_VALUE);
            this.numbers = numbers;
        }

        @Override
        public boolean isFinished() {
            return containsAllNumbersOfSequence();
        }

        @Override
        public long missing() {
            Set<Long> sequence = LongStream.range(0, numbers).boxed().collect(toUnmodifiableSet());
            Set<Long> values = acceptedValues.stream()
                    .filter(this::isInteger).map(Long::parseLong).collect(Collectors.toSet());
            return sequence.size() - values.size();
        }

        private boolean containsAllNumbersOfSequence() {
            List<Long> sequence = LongStream.range(0, numbers).boxed().collect(toUnmodifiableList());
            List<Long> values = acceptedValues.stream()
                    .filter(this::isInteger).map(Long::parseLong).collect(toList());
            return values.containsAll(sequence);
        }

        private boolean isInteger(String value) {
            return value != null && value.matches("\\d+");
        }
    }

    public static class ValidRecordReceived extends RecordReceived {

        public ValidRecordReceived(String clientName, ConsumerRecord<String, String> record) {
            super(clientName, record);
        }

        @Override
        public String name() {
            return "valid_record_received";
        }
    }

    public static class UnexpectedRecordReceived extends RecordReceived {

        public UnexpectedRecordReceived(String clientName, ConsumerRecord<String, String> record) {
            super(clientName, record);
        }

        @Override
        public String name() {
            return "unexpected_record_received";
        }
    }

    public static class RecordVerificationSummary extends ClientEvent {

        private String key;

        private long values;

        private long duplicates;

        private long missing;

        private long invalid;

        private boolean outOfSequence;

        public RecordVerificationSummary(String clientName, String key, long values, long duplicates, long missing, long invalid, boolean outOfSequence) {
            super(clientName);
            this.key = key;
            this.values = values;
            this.duplicates = duplicates;
            this.missing = missing;
            this.invalid = invalid;
            this.outOfSequence = outOfSequence;
        }

        @Override
        public String name() {
            return "record_verification_summary";
        }

        @JsonProperty
        public String key() {
            return key;
        }

        @JsonProperty
        public boolean success() {
            return duplicates == 0 && missing == 0 && invalid == 0 && !outOfSequence;
        }

        @JsonProperty
        public long values() {
            return values;
        }

        @JsonProperty
        public long duplicates() {
            return duplicates;
        }

        @JsonProperty
        public long missing() {
            return missing;
        }

        @JsonProperty
        public long invalid() {
            return invalid;
        }

        @JsonProperty
        public boolean outOfSequence() {
            return outOfSequence;
        }
    }

    public static class PartitionsRevoked extends ClientEvent {
        private final Collection<TopicPartition> partitions;

        public PartitionsRevoked(String clientName, Collection<TopicPartition> partitions) {
            super(clientName);
            this.partitions = partitions;
        }

        @JsonProperty
        public Collection<TopicPartition> partitions() {
            return partitions;
        }

        @Override
        public String name() {
            return "partitions_revoked";
        }
    }

    public static class PartitionsAssigned extends ClientEvent {
        private final Collection<TopicPartition> partitions;

        public PartitionsAssigned(String clientName, Collection<TopicPartition> partitions) {
            super(clientName);
            this.partitions = partitions;
        }

        @JsonProperty
        public Collection<TopicPartition> partitions() {
            return partitions;
        }

        @Override
        public String name() {
            return "partitions_assigned";
        }
    }

    public static class RecordsConsumed extends ClientEvent {
        private final long count;
        private final long accepted;
        private final List<RecordSetSummary> partitionSummaries;

        public RecordsConsumed(String clientName, long count, long accepted, List<RecordSetSummary> partitionSummaries) {
            super(clientName);
            this.count = count;
            this.accepted = accepted;
            this.partitionSummaries = partitionSummaries;
        }

        @Override
        public String name() {
            return "records_consumed";
        }

        @JsonProperty
        public long count() {
            return count;
        }

        @JsonProperty
        public long accepted() {
            return accepted;
        }

        @JsonProperty
        public List<RecordSetSummary> partitions() {
            return partitionSummaries;
        }
    }

    @JsonPropertyOrder({ "timestamp", "name", "key", "value", "topic", "partition", "offset" })
    public static class RecordReceived extends ClientEvent {

        private final ConsumerRecord<String, String> record;

        public RecordReceived(String clientName, ConsumerRecord<String, String> record) {
            super(clientName);
            this.record = record;
        }

        @Override
        public String name() {
            return "record_received";
        }

        @JsonProperty
        public String topic() {
            return record.topic();
        }

        @JsonProperty
        public int partition() {
            return record.partition();
        }

        @JsonProperty
        public String key() {
            return record.key();
        }

        @JsonProperty
        public String value() {
            return record.value();
        }

        @JsonProperty
        public long offset() {
            return record.offset();
        }

    }

    public static class PartitionData {
        private final String topic;
        private final int partition;

        public PartitionData(String topic, int partition) {
            this.topic = topic;
            this.partition = partition;
        }

        @JsonProperty
        public String topic() {
            return topic;
        }

        @JsonProperty
        public int partition() {
            return partition;
        }
    }

    public static class OffsetsCommitted extends ClientEvent {

        private final List<CommitData> offsets;
        private final String error;
        private final boolean success;

        public OffsetsCommitted(String clientName, List<CommitData> offsets, String error, boolean success) {
            super(clientName);
            this.offsets = offsets;
            this.error = error;
            this.success = success;
        }

        @Override
        public String name() {
            return "offsets_committed";
        }

        @JsonProperty
        public List<CommitData> offsets() {
            return offsets;
        }

        @JsonProperty
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public String error() {
            return error;
        }

        @JsonProperty
        public boolean success() {
            return success;
        }

    }

    public static class CommitData extends PartitionData {
        private final long offset;

        public CommitData(String topic, int partition, long offset) {
            super(topic, partition);
            this.offset = offset;
        }

        @JsonProperty
        public long offset() {
            return offset;
        }
    }

    public static class RecordSetSummary extends PartitionData {
        private final long count;
        private final long accepted;
        private final long minOffset;
        private final long maxOffset;

        public RecordSetSummary(String topic, int partition, long count, long accepted, long minOffset, long maxOffset) {
            super(topic, partition);
            this.count = count;
            this.accepted = accepted;
            this.minOffset = minOffset;
            this.maxOffset = maxOffset;
        }

        @JsonProperty
        public long count() {
            return count;
        }

        @JsonProperty
        public long accepted() {
            return accepted;
        }

        @JsonProperty
        public long minOffset() {
            return minOffset;
        }

        @JsonProperty
        public long maxOffset() {
            return maxOffset;
        }

    }

}
