package net.uweeisele.examples.kafka.sequence;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Exit;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static net.sourceforge.argparse4j.impl.Arguments.*;

public class VerifiedGeneratorCommand {

    public static void main(String[] args) {
        ArgumentParser parser = argParser();
        if (args.length == 0) {
            parser.printHelp();
            Exit.exit(0);
        }

        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            parser.printHelp();
            Exit.exit(1);
        }

        List<String> keys = generateKeys(res);

        int returnCode = 0;
        try(Generator generator = createGeneratorFromArgs(res, keys);
            Verifier verifier = createVerifierFromArgs(res, keys)) {
            getRuntime().addShutdownHook(new Thread(generator::close));
            getRuntime().addShutdownHook(new Thread(verifier::close));

            Executors.newSingleThreadScheduledExecutor(daemonThreadFactory()).schedule(() -> {
                generator.close();
                verifier.close();
            }, timeoutMsFromArgs(res), MILLISECONDS);

            ExecutorService executorService = Executors.newFixedThreadPool(2);

            executorService.submit(generator);
            executorService.submit(verifier);

            executorService.shutdown();
            executorService.awaitTermination(Long.MAX_VALUE, MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            returnCode = 2;
        }

        Exit.exit(returnCode);
    }

    private static List<String> generateKeys(Namespace res) {
        List<String> keys = res.getList("keys");
        if (keys == null) {
            Integer numKeys = res.getInt("numKeys");
            keys = new ArrayList<>();
            if (numKeys <= 0) {
                keys.add(null);
            } else {
                for (int k = 0; k < numKeys; k++) {
                    keys.add(randomUUID().toString());
                }
            }
        }
        return keys;
    }

    private static Generator createGeneratorFromArgs(Namespace res, Collection<String> keys) {
        String brokerList = res.getString("brokerList");
        String topic = res.getString("topicProduce");
        int acks = res.getInt("acks");
        boolean sync = res.getBoolean("sync");
        long maxMessagesPerKey = res.getLong("maxMessagesPerKey");
        int throughput = res.getInt("throughput");
        String createTime = res.getString("createTime");

        if (createTime != null && createTime.isBlank()) {
            createTime = null;
        }

        Properties actualProducerProperties = new Properties();
        Optional.<Properties>ofNullable(res.get("producer.config")).ifPresent(actualProducerProperties::putAll);

        actualProducerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        actualProducerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        actualProducerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        actualProducerProperties.setProperty(ProducerConfig.ACKS_CONFIG, String.valueOf(acks));

        return new Generator.Builder(new KafkaProducer<>(actualProducerProperties), topic)
                .withKeys(keys)
                .withMaxMessagesPerKey(maxMessagesPerKey)
                .withThroughput(new ThroughputThrottler(throughput))
                .withCreateTime(createTime != null ? Instant.parse(createTime) : null)
                .withSyncProduce(sync)
                .build();
    }

    private static Verifier createVerifierFromArgs(Namespace res, Collection<String> keys) {
        String topic = res.getString("topicConsume");
        boolean useAutoCommit = res.getBoolean("useAutoCommit");

        Properties actualConsumerProperties = new Properties();
        Optional.<Properties>ofNullable(res.get("consumer.config")).ifPresent(actualConsumerProperties::putAll);

        String groupId = res.getString("groupId");
        actualConsumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        String groupInstanceId = res.getString("groupInstanceId");
        if (groupInstanceId != null) {
            actualConsumerProperties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId);
        }
        actualConsumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, res.getString("brokerList"));
        actualConsumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, useAutoCommit);
        actualConsumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, res.getString("resetPolicy"));
        actualConsumerProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(res.getInt("sessionTimeout")));
        actualConsumerProperties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, res.getString("assignmentStrategy"));

        StringDeserializer deserializer = new StringDeserializer();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(actualConsumerProperties, deserializer, deserializer);

        final long sequenceNumbers = res.getLong("maxMessagesPerKey") >= 0 ? res.getLong("maxMessagesPerKey") : Long.MAX_VALUE;

        return new Verifier.Builder(consumer, topic)
                .withClientName(groupId)
                .withAsyncCommit(false)
                .withAutoCommit(useAutoCommit)
                .withKeyFilterSupplier(() -> new Verifier.FixedValueFilter(new HashSet<>(keys)))
                .withValueFilterSupplier(() -> new Verifier.SequenceValueFilter(sequenceNumbers))
                .build();
    }

    private static ThreadFactory daemonThreadFactory() {
        return r -> {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setDaemon(true);
            return t;
        };
    }

    private static long timeoutMsFromArgs(Namespace res) {
        return res.getInt("maxDuration") >= 0 ? res.getInt("maxDuration") : Long.MAX_VALUE;
    }

    /** Get the command-line argument parser. */
    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newFor("verified-generator")
                .addHelp(true)
                .build()
                .description("This tool produces increasing integers to the specified topic and prints JSON metadata to stdout on each \"send\" request, making externally visible which messages have been acked and which have not. " +
                        "In addition this tool consumes messages from a specific topic and emits consumer events (e.g. group rebalances, received messages, and offsets committed) as JSON objects to STDOUT.");

        argParserCommon(parser);
        argParserGenerator(parser.addArgumentGroup("Generator"));
        argParserVerifier(parser.addArgumentGroup("Verifier"));

        return parser;
    }

    private static void argParserCommon(ArgumentParser parser) {
        parser.addArgument("--broker-list")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("HOST1:PORT1[,HOST2:PORT2[...]]")
                .dest("brokerList")
                .help("Comma-separated list of Kafka brokers in the form HOST1:PORT1,HOST2:PORT2,...");

        MutuallyExclusiveGroup keyGroup = parser.addMutuallyExclusiveGroup()
                .required(false)
                .description("Keys can be specified explicitly or by specifying the number of keys. Only one type of this options may be used.");
        keyGroup.addArgument("--num-keys")
                .action(store())
                .required(false)
                .setDefault(1)
                .type(Integer.class)
                .metavar("NUM-KEYS")
                .dest("numKeys")
                .help("Produce messages for this many keys. If 0 or less, null is used as key. Default is 1 key.");
        keyGroup.addArgument("--key")
                .action(append())
                .required(false)
                .type(String.class)
                .metavar("KEY")
                .dest("keys")
                .help("Key which should be used for generated sequence. Argument can be specified multiple times.");

        parser.addArgument("--max-messages-per-key")
                .action(store())
                .required(false)
                .setDefault(-1)
                .type(Long.class)
                .metavar("MAX-MESSAGES-PER-KEYS")
                .dest("maxMessagesPerKey")
                .help("Produce this many messages per key. If -1, produce messages until the process is killed externally. Default is -1.");

        parser.addArgument("--max-duration")
                .action(store())
                .required(false)
                .setDefault(-1)
                .type(Integer.class)
                .metavar("DURATION_MS")
                .dest("maxDuration")
                .help("Set the maximum duration for the generation and verification in milliseconds. If this value is negative, the max duration is infinity. Default is -1.");
    }

    private static void argParserGenerator(ArgumentContainer parser) {
        parser.addArgument("--topic-produce")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("TOPIC-PRODUCE")
                .dest("topicProduce")
                .help("Produce messages to this topic.");

        parser.addArgument("--producer.config")
                .action(store())
                .required(false)
                .type(new PropertiesFileArgumentType())
                .metavar("CONFIG_FILE_PRODUCER")
                .help("Producer config properties file.");

        parser.addArgument("--throughput")
                .action(store())
                .required(false)
                .setDefault(-1)
                .type(Integer.class)
                .metavar("THROUGHPUT")
                .help("If set >= 0, throttle maximum message throughput to *approximately* THROUGHPUT messages/sec.");

        parser.addArgument("--acks")
                .action(store())
                .required(false)
                .setDefault(-1)
                .type(Integer.class)
                .choices(0, 1, -1)
                .metavar("ACKS")
                .help("Acks required on each produced message. See Kafka docs on acks for details.");

        parser.addArgument("--sync")
                .action(store())
                .required(false)
                .setDefault(false)
                .type(Boolean.class)
                .metavar("SYNC")
                .help("Synchronously produce records.");

        parser.addArgument("--message-create-time")
                .action(store())
                .required(false)
                .setDefault((String)null)
                .type(String.class)
                .metavar("CREATETIME")
                .dest("createTime")
                .help("Send messages with creation time starting at the arguments value, represented as ISO Instant string such as  2019-07-26T10:15:30.00Z.");
    }

    private static void argParserVerifier(ArgumentContainer parser) {
        parser.addArgument("--topic-consume")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("TOPIC-CONSUME")
                .dest("topicConsume")
                .help("Consumes messages from this topic.");

        parser.addArgument("--consumer.config")
                .action(store())
                .required(false)
                .type(new PropertiesFileArgumentType())
                .metavar("CONFIG_FILE_CONSUMER")
                .help("Consumer config properties file (config options shared with command line parameters will be overridden).");

        parser.addArgument("--session-timeout")
                .action(store())
                .required(false)
                .setDefault(30000)
                .type(Integer.class)
                .metavar("TIMEOUT_MS")
                .dest("sessionTimeout")
                .help("Set the consumer's session timeout");

        parser.addArgument("--group-id")
                .action(store())
                .required(false)
                .setDefault(format("verifier-%s", randomUUID()))
                .type(String.class)
                .metavar("GROUP_ID")
                .dest("groupId")
                .help("The groupId shared among members of the consumer group");

        parser.addArgument("--group-instance-id")
                .action(store())
                .required(false)
                .type(String.class)
                .setDefault((String)null)
                .metavar("GROUP_INSTANCE_ID")
                .dest("groupInstanceId")
                .help("A unique identifier of the consumer instance");

        parser.addArgument("--enable-autocommit")
                .action(storeTrue())
                .type(Boolean.class)
                .metavar("ENABLE-AUTOCOMMIT")
                .dest("useAutoCommit")
                .help("Enable offset auto-commit on consumer");

        parser.addArgument("--reset-policy")
                .action(store())
                .required(false)
                .setDefault("earliest")
                .type(String.class)
                .dest("resetPolicy")
                .help("Set reset policy (must be either 'earliest', 'latest', or 'none'. Default is earliest.");

        parser.addArgument("--assignment-strategy")
                .action(store())
                .required(false)
                .setDefault(RangeAssignor.class.getName())
                .type(String.class)
                .dest("assignmentStrategy")
                .help("Set assignment strategy (e.g. " + RoundRobinAssignor.class.getName() + ")");
    }

}
