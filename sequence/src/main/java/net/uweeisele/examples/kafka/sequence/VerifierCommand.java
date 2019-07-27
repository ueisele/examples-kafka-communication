package net.uweeisele.examples.kafka.sequence;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class VerifierCommand {

    public static void main(String[] args) {
        ArgumentParser parser = argParser();
        if (args.length == 0) {
            parser.printHelp();
            Exit.exit(0);
        }

        try {
            final Verifier verifier = createFromArgs(parser, args);
            Runtime.getRuntime().addShutdownHook(new Thread(verifier::close));
            Executors.newSingleThreadScheduledExecutor(daemonThreadFactory()).schedule(verifier::close, timeoutMsFromArgs(parser, args), MILLISECONDS);

            verifier.run();
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            Exit.exit(1);
        }
    }

    private static ThreadFactory daemonThreadFactory() {
        return r -> {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setDaemon(true);
            return t;
        };
    }

    private static Verifier createFromArgs(ArgumentParser parser, String[] args) throws ArgumentParserException {
        Namespace res = parser.parseArgs(args);

        String topic = res.getString("topic");
        boolean useAutoCommit = res.getBoolean("useAutoCommit");
        String configFile = res.getString("consumer.config");

        Properties consumerProps = new Properties();
        if (configFile != null) {
            try {
                consumerProps.putAll(Utils.loadProps(configFile));
            } catch (IOException e) {
                throw new ArgumentParserException(e.getMessage(), parser);
            }
        }

        final long sequenceNumbers = res.getLong("sequenceNumbers") >= 0 ? res.getLong("sequenceNumbers") : Long.MAX_VALUE;

        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, res.getString("groupId"));

        String groupInstanceId = res.getString("groupInstanceId");
        if (groupInstanceId != null) {
            consumerProps.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId);
        }
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, res.getString("brokerList"));
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, useAutoCommit);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, res.getString("resetPolicy"));
        consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(res.getInt("sessionTimeout")));
        consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, res.getString("assignmentStrategy"));

        StringDeserializer deserializer = new StringDeserializer();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps, deserializer, deserializer);

        Supplier<Verifier.ValueFilter> keyFilterSupplier;
        String keys = res.getString("keys");
        if (keys != null) {
           keyFilterSupplier = () -> new Verifier.FixedValueFilter(new HashSet<>(asList(keys.split(","))));
        } else {
            keyFilterSupplier = () -> new Verifier.DistinctMaxAcceptedValueFilter(res.getLong("maxKeys"));
        }

        return new Verifier.Builder(consumer, topic)
                .withAsyncCommit(false)
                .withAutoCommit(useAutoCommit)
                .withKeyFilterSupplier(keyFilterSupplier)
                .withValueFilterSupplier(() -> new Verifier.SequenceValueFilter(sequenceNumbers))
                .build();
    }

    private static long timeoutMsFromArgs(ArgumentParser parser, String[] args) throws ArgumentParserException {
        Namespace res = parser.parseArgs(args);
        return res.getInt("maxDuration");

    }

    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newFor("verifier")
                .addHelp(true)
                .build()
                .description("This tool consumes messages from a specific topic and emits consumer events (e.g. group rebalances, received messages, and offsets committed) as JSON objects to STDOUT.");

        parser.addArgument("--topic")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("TOPIC")
                .help("Consumes messages from this topic.");

        parser.addArgument("--broker-list")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("HOST1:PORT1[,HOST2:PORT2[...]]")
                .dest("brokerList")
                .help("Comma-separated list of Kafka brokers in the form HOST1:PORT1,HOST2:PORT2,...");

        parser.addArgument("--group-id")
                .action(store())
                .required(true)
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

        parser.addArgument("--keys")
                .action(store())
                .required(false)
                .type(String.class)
                .setDefault((String)null)
                .metavar("key1,key2,...")
                .help("Comma-separated list of keys which should be consumed in the form key1,key2,...");

        parser.addArgument("--max-keys")
                .action(store())
                .required(false)
                .type(Long.class)
                .setDefault(1)
                .metavar("MAX-KEYS")
                .dest("maxKeys")
                .help("The maximum amount of different keys which are consumed. Default is 1. If -1, the consumer will consume until the process is killed externally");

        parser.addArgument("--sequence-numbers")
                .action(store())
                .required(false)
                .type(Long.class)
                .setDefault(-1)
                .metavar("SEQUENCE-NUMBERS")
                .dest("sequenceNumbers")
                .help("Consume until a sequence with the given amount of numbers has been consumed. If -1 (the default), the consumer will consume until the process is killed externally");

        parser.addArgument("--max-duration")
                .action(store())
                .required(false)
                .setDefault(60000)
                .type(Integer.class)
                .metavar("DURATION_MS")
                .dest("maxDuration")
                .help("Set the maximum duration for the verification. Default is 60 seconds.");

        parser.addArgument("--session-timeout")
                .action(store())
                .required(false)
                .setDefault(30000)
                .type(Integer.class)
                .metavar("TIMEOUT_MS")
                .dest("sessionTimeout")
                .help("Set the consumer's session timeout");

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
                .help("Set reset policy (must be either 'earliest', 'latest', or 'none'");

        parser.addArgument("--assignment-strategy")
                .action(store())
                .required(false)
                .setDefault(RangeAssignor.class.getName())
                .type(String.class)
                .dest("assignmentStrategy")
                .help("Set assignment strategy (e.g. " + RoundRobinAssignor.class.getName() + ")");

        parser.addArgument("--consumer.config")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("CONFIG_FILE")
                .help("Consumer config properties file (config options shared with command line parameters will be overridden).");

        return parser;
    }

}
