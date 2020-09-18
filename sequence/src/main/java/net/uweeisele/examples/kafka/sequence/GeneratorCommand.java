package net.uweeisele.examples.kafka.sequence;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Exit;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static java.util.Optional.ofNullable;
import static net.sourceforge.argparse4j.impl.Arguments.append;
import static net.sourceforge.argparse4j.impl.Arguments.store;

public class GeneratorCommand {

    public static void main(String[] args) {
        ArgumentParser parser = argParser();
        if (args.length == 0) {
            parser.printHelp();
            Exit.exit(0);
        }

        try {
            final Generator generator = createFromArgs(parser, args);
            Runtime.getRuntime().addShutdownHook(new Thread(generator::close));

            generator.run();
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            Exit.exit(1);
        }
    }

    private static Generator createFromArgs(ArgumentParser parser, String[] args) throws ArgumentParserException {
        Namespace res = parser.parseArgs(args);

        String brokerList = res.getString("brokerList");
        String topic = res.getString("topic");
        String clientName = res.getString("clientName");
        int acks = res.getInt("acks");
        boolean sync = res.getBoolean("sync");
        Integer numKeys = res.getInt("numKeys");
        List<String> keys = res.getList("keys");
        long maxMessagesPerKey = res.getLong("maxMessagesPerKey");
        int throughput = res.getInt("throughput");
        String createTime = res.getString("createTime");

        if (createTime != null && createTime.isBlank()) {
            createTime = null;
        }

        Properties actualProducerProperties = new Properties();
        Optional.<Properties>ofNullable(res.get("producer.config")).ifPresent(actualProducerProperties::putAll);

        actualProducerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        actualProducerProperties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, clientName);
        actualProducerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        actualProducerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        actualProducerProperties.setProperty(ProducerConfig.ACKS_CONFIG, String.valueOf(acks));

        Generator.Builder builder = new Generator.Builder(new KafkaProducer<>(actualProducerProperties), topic)
                .withClientName(clientName)
                .withMaxMessagesPerKey(maxMessagesPerKey)
                .withThroughput(new ThroughputThrottler(throughput))
                .withCreateTime(createTime != null ? Instant.parse(createTime) : null)
                .withSyncProduce(sync);
        ofNullable(numKeys).ifPresent(builder::withNumKeys);
        ofNullable(keys).ifPresent(builder::withKeys);

        return builder.build();
    }

    /** Get the command-line argument parser. */
    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newFor("generator")
                .addHelp(true)
                .build()
                .description("This tool produces increasing integers to the specified topic and prints JSON metadata to stdout on each \"send\" request, making externally visible which messages have been acked and which have not.");

        parser.addArgument("--topic")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("TOPIC")
                .help("Produce messages to this topic.");

        parser.addArgument("--client-name")
                .action(store())
                .required(false)
                .type(String.class)
                .setDefault("generator")
                .metavar("CLIENT-NAME")
                .dest("clientName")
                .help("Name of the producer.");

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
                .type(Integer.class)
                .metavar("NUM-KEYS")
                .dest("numKeys")
                .help("Produce messages for this many keys. If 0 or less, null is used as key.");
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
                .help("Produce this many messages per key. If -1, produce messages until the process is killed externally.");

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

        parser.addArgument("--producer.config")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("CONFIG_FILE")
                .help("Producer config properties file (config options shared with command line parameters will be overridden).");

        parser.addArgument("--message-create-time")
                .action(store())
                .required(false)
                .setDefault((String)null)
                .type(String.class)
                .metavar("CREATETIME")
                .dest("createTime")
                .help("Send messages with creation time starting at the arguments value, represented as ISO Instant string such as  2019-07-26T10:15:30.00Z.");

        return parser;
    }

}
