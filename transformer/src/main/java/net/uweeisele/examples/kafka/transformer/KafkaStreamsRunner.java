package net.uweeisele.examples.kafka.transformer;

import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static net.uweeisele.examples.kafka.transformer.ErrorCode.Severity.INFO;
import static net.uweeisele.examples.kafka.transformer.ReturnCode.internalCode;
import static net.uweeisele.examples.kafka.transformer.ReturnCode.success;

public class KafkaStreamsRunner implements Callable<Integer>, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsRunner.class);

    private static final ErrorCode INTERRUPTED_EXIT = new ErrorCode("Application has been interrupted! Stopping now!", internalCode(254), INFO);

    private final Function<Properties, KafkaStreams> kafkaStreamsBuilder;
    private final Supplier<Properties> propertiesSupplier;

    private final Function<Exception, ErrorCode> errorCodeFactory;

    private final Runtime runtime;
    private final Function<Runnable, Thread> shutdownHookThreadFactory;

    private final AtomicBoolean hasStarted = new AtomicBoolean(false);
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private final CountDownLatch shutdownCompletedLatch = new CountDownLatch(1);

    public KafkaStreamsRunner(Function<Properties, KafkaStreams> kafkaStreamsBuilder) {
        this(kafkaStreamsBuilder, Properties::new);
    }

    public KafkaStreamsRunner(Function<Properties, KafkaStreams> kafkaStreamsBuilder, Supplier<Properties> propertiesSupplier) {
        this(kafkaStreamsBuilder, propertiesSupplier, new DefaultErrorCodeFactory());
    }

    public KafkaStreamsRunner(Function<Properties, KafkaStreams> kafkaStreamsBuilder, Supplier<Properties> propertiesSupplier, Function<Exception, ErrorCode> errorCodeFactory) {
        this(kafkaStreamsBuilder, propertiesSupplier, errorCodeFactory, Runtime.getRuntime(), Thread::new);
    }

    public KafkaStreamsRunner(Function<Properties, KafkaStreams> kafkaStreamsBuilder, Supplier<Properties> propertiesSupplier, Function<Exception, ErrorCode> errorCodeFactory, Runtime runtime, Function<Runnable, Thread> shutdownHookThreadFactory) {
        this.kafkaStreamsBuilder = requireNonNull(kafkaStreamsBuilder);
        this.propertiesSupplier = requireNonNull(propertiesSupplier);
        this.errorCodeFactory = requireNonNull(errorCodeFactory);
        this.runtime = requireNonNull(runtime);
        this.shutdownHookThreadFactory = requireNonNull(shutdownHookThreadFactory);
    }

    @Override
    public Integer call() throws IllegalStateException {
        assertOnlyCalledOnce();

        registerShutdownHook();

        ReturnCode returnCode;
        try(KafkaStreams kafkaStreams = kafkaStreamsBuilder.apply(propertiesSupplier.get())) {
            kafkaStreams.start();
            try {
                shutdownLatch.await();
                returnCode = success();
            } catch (InterruptedException interruptedException) {
                returnCode = handleInterruptedException(interruptedException);
            }
        } catch (Exception exception) {
            returnCode = handleException(exception);
        } finally {
            shutdownCompletedLatch.countDown();
        }
        return returnCode.get();
    }

    @Override
    public void close() {
        boolean interrupted = false;
        try {
            shutdownLatch.countDown();
            while (hasStarted.get()) {
                try {
                    shutdownCompletedLatch.await();
                    return;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void assertOnlyCalledOnce() throws IllegalStateException {
        if (hasStarted.compareAndExchange(false, true)) {
            throw new IllegalStateException("Application has already be executed!");
        }
    }

    private void registerShutdownHook() {
        Thread closeAction = shutdownHookThreadFactory.apply(this::close);
        try {
            runtime.addShutdownHook(closeAction);
        } catch (IllegalStateException exception) {
            LOG.info("Application is already in the process of shutting down. Instantly initiating close action!");
            closeAction.start();
        }
    }

    private ReturnCode handleInterruptedException(InterruptedException e) {
        Thread.currentThread().interrupt();
        log(INTERRUPTED_EXIT, e);
        return INTERRUPTED_EXIT.getCode();
    }

    private ReturnCode handleException(Exception e) {
        ErrorCode errorCode = errorCodeFactory.apply(e);
        log(errorCode, e);
        return errorCode.getCode();
    }

    private void log(ErrorCode code, Exception e) {
        logger(code).accept(code.getMessage(), e);
    }

    private BiConsumer<String, Exception> logger(ErrorCode code) {
        switch (code.getSeverity()) {
            case INFO:
                return logger(code, (msg, e) -> LOG.info(msg), LOG::info);
            case WARNING:
                return logger(code, (msg, e) -> LOG.warn(msg), LOG::warn);
            default:
                return logger(code, LOG::error, LOG::error);
        }
    }

    private BiConsumer<String, Exception> logger(ErrorCode code, BiConsumer<String, Exception> logAction, BiConsumer<String, Exception> debugAction) {
        return (msg, e) -> {
            if (!LOG.isDebugEnabled() && !code.isSuppressMessage()) {
                logAction.accept(msg, e);
            } else if (LOG.isDebugEnabled() && !code.isSuppressMessage()) {
                debugAction.accept(msg, e);
            } else if (LOG.isDebugEnabled() && code.isSuppressMessage()) {
                LOG.debug(msg, e);
            }
        };
    }

}
