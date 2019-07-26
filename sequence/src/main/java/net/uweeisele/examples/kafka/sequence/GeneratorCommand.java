package net.uweeisele.examples.kafka.sequence;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Exit;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Properties;

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

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                // Trigger main thread to stop producing messages
                generator.stopProducing();

                // Flush any remaining messages
                generator.close();
            }));

            generator.run();
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            Exit.exit(1);
        }
    }

    public static Generator createFromArgs(ArgumentParser parser, String[] args) throws ArgumentParserException {
        Namespace res = parser.parseArgs(args);

        String brokerList = res.getString("brokerList");
        String topic = res.getString("topic");
        int acks = res.getInt("acks");
        String configFile = res.getString("producer.config");
        int numKeys = res.getInt("numKeys");
        long maxMessagesPerKey = res.getLong("maxMessagesPerKey");
        int throughput = res.getInt("throughput");
        String createTime = res.getString("createTime");

        if (createTime != null && createTime.isBlank()) {
            createTime = null;
        }

        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.setProperty(ProducerConfig.ACKS_CONFIG, String.valueOf(acks));
        producerProps.setProperty(ProducerConfig.RETRIES_CONFIG, "0");
        if (configFile != null) {
            try {
                producerProps.putAll(loadProps(configFile));
            } catch (IOException e) {
                throw new ArgumentParserException(e.getMessage(), parser);
            }
        }

        return new Generator.Builder(new KafkaProducer<>(producerProps), topic)
                .withNumKeys(numKeys)
                .withMaxMessagesPerKey(maxMessagesPerKey)
                .withThroughput(new ThroughputThrottler(throughput))
                .withCreateTime(createTime != null ? Instant.parse(createTime) : null)
                .build();
    }

    /** Get the command-line argument parser. */
    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newFor("verifiable-producer")
                .addHelp(true)
                .build()
                .description("This tool produces increasing integers to the specified topic and prints JSON metadata to stdout on each \"send\" request, making externally visible which messages have been acked and which have not.");

        parser.addArgument("--topic")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("TOPIC")
                .help("Produce messages to this topic.");

        parser.addArgument("--broker-list")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("HOST1:PORT1[,HOST2:PORT2[...]]")
                .dest("brokerList")
                .help("Comma-separated list of Kafka brokers in the form HOST1:PORT1,HOST2:PORT2,...");

        parser.addArgument("--num-keys")
                .action(store())
                .required(false)
                .setDefault(1)
                .type(Integer.class)
                .metavar("NUM-KEYS")
                .dest("numKeys")
                .help("Produce messages for this many keys. If 0 or less, null is used as key.");

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

        parser.addArgument("--producer.config")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("CONFIG_FILE")
                .help("Producer config properties file.");

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

    /**
     * Read a properties file from the given path
     * @param filename The path of the file to read
     *
     * Note: this duplication of org.apache.kafka.common.utils.Utils.loadProps is unfortunate
     * but *intentional*. In order to use VerifiableProducer in compatibility and upgrade tests,
     * we use VerifiableProducer from the development tools package, and run it against 0.8.X.X kafka jars.
     * Since this method is not in Utils in the 0.8.X.X jars, we have to cheat a bit and duplicate.
     */
    public static Properties loadProps(String filename) throws IOException {
        Properties props = new Properties();
        try (InputStream propStream = Files.newInputStream(Paths.get(filename))) {
            props.load(propStream);
        }
        return props;
    }
}
