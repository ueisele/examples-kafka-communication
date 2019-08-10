package net.uweeisele.examples.kafka.transformer;

import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static net.uweeisele.examples.kafka.transformer.ErrorCode.Severity.INFO;
import static net.uweeisele.examples.kafka.transformer.ReturnCode.internalCode;
import static net.uweeisele.examples.kafka.transformer.ReturnCode.success;
import static net.uweeisele.examples.kafka.transformer.ShutdownHandler.SIGNAL;

public class KafkaStreamsRunner implements Callable<Integer>, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsRunner.class);

    private static final ErrorCode INTERRUPTED_EXIT = new ErrorCode("Application has been interrupted! Stopping now!", internalCode(255), INFO);

    private final Function<Properties, KafkaStreams> kafkaStreamsBuilder;
    private final Supplier<Properties> propertiesSupplier;

    private final Function<Throwable, ErrorCode> errorCodeFactory;

    private final BiConsumer<Signal, SignalHandler> signalHandlerRegistry;
    private final Runtime runtime;
    private final Function<Runnable, Thread> shutdownHookThreadFactory;

    private final AtomicBoolean hasStarted = new AtomicBoolean(false);
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private final CountDownLatch shutdownCompletedLatch = new CountDownLatch(1);

    private ShutdownHandler shutdownHandler = SIGNAL;

    public KafkaStreamsRunner(Function<Properties, KafkaStreams> kafkaStreamsBuilder) {
        this(kafkaStreamsBuilder, Properties::new);
    }

    public KafkaStreamsRunner(Function<Properties, KafkaStreams> kafkaStreamsBuilder, Supplier<Properties> propertiesSupplier) {
        this(kafkaStreamsBuilder, propertiesSupplier, new DefaultErrorCodeFactory());
    }

    public KafkaStreamsRunner(Function<Properties, KafkaStreams> kafkaStreamsBuilder, Supplier<Properties> propertiesSupplier, Function<Throwable, ErrorCode> errorCodeFactory) {
        this(kafkaStreamsBuilder, propertiesSupplier, errorCodeFactory, Signal::handle, Runtime.getRuntime(), Thread::new);
    }

    public KafkaStreamsRunner(Function<Properties, KafkaStreams> kafkaStreamsBuilder,
                              Supplier<Properties> propertiesSupplier,
                              Function<Throwable, ErrorCode> errorCodeFactory,
                              BiConsumer<Signal, SignalHandler> signalHandlerRegistry,
                              Runtime runtime,
                              Function<Runnable, Thread> shutdownHookThreadFactory) {
        this.kafkaStreamsBuilder = requireNonNull(kafkaStreamsBuilder);
        this.propertiesSupplier = requireNonNull(propertiesSupplier);
        this.errorCodeFactory = requireNonNull(errorCodeFactory);
        this.signalHandlerRegistry = requireNonNull(signalHandlerRegistry);
        this.runtime = requireNonNull(runtime);
        this.shutdownHookThreadFactory = requireNonNull(shutdownHookThreadFactory);
    }

    @Override
    public Integer call() throws IllegalStateException {
        assertOnlyCalledOnce();

        registerShutdownHandling();

        AtomicReference<ReturnCode> returnCode = new AtomicReference<>(success());
        try(KafkaStreams kafkaStreams = kafkaStreamsBuilder.apply(propertiesSupplier.get())) {
            closeOnUncaughtException(kafkaStreams, returnCode);
            kafkaStreams.start();
            try {
                shutdownLatch.await();
            } catch (InterruptedException interruptedException) {
                returnCode.set(handleInterruptedException(interruptedException));
            }
        } catch (Exception exception) {
            returnCode.set(handleException(exception));
        } finally {
            shutdownCompletedLatch.countDown();
        }
        return returnCode.get().get();
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

    /**
     * Default shutdown handling is done via signal handler.
     * This signal handler the signals INT and TERM lead to a successful termination with exit code 0.
     * However, using signal handler also means, that no other shutdown hooks are executed.
     * Hook handling leads to a unsuccessful termination with exit code non 0.
     *
     * @param shutdownHandler the handling to use
     * @return this {@link KafkaStreamsRunner}
     */
    public KafkaStreamsRunner withShutdownHandler(ShutdownHandler shutdownHandler) {
        this.shutdownHandler = requireNonNull(shutdownHandler);
        return this;
    }

    private void assertOnlyCalledOnce() throws IllegalStateException {
        if (hasStarted.compareAndExchange(false, true)) {
            throw new IllegalStateException("Application has already be executed!");
        }
    }

    private void registerShutdownHandling() {
        switch (shutdownHandler) {
            case SIGNAL:
                registerShutdownSignalHandler();
                break;
            case HOOK:
                registerShutdownHook();
                break;
            case NONE:
                break;
        }
    }

    private void registerShutdownSignalHandler() {
        // Signal handler enable a graceful shutdown of the application.
        // The application is not terminated (like it is when just using shutdown hooks), but rather normally closed.
        SignalHandler signalHandler = sig -> {
            LOG.info(String.format("Closing application because of %s signal.", sig.getName()));
            shutdownHookThreadFactory.apply(KafkaStreamsRunner.this::close).start();
        };
        signalHandlerRegistry.accept(new Signal("TERM"), signalHandler);
        signalHandlerRegistry.accept(new Signal("INT"), signalHandler);
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

    private void closeOnUncaughtException(KafkaStreams kafkaStreams, AtomicReference<ReturnCode> returnCode) {
        kafkaStreams.setUncaughtExceptionHandler((t, e) -> {
            returnCode.set(handleException(e));
            shutdownHookThreadFactory.apply(this::close).start();
        });
    }

    private ReturnCode handleInterruptedException(InterruptedException e) {
        Thread.currentThread().interrupt();
        log(INTERRUPTED_EXIT, e);
        return INTERRUPTED_EXIT.getCode();
    }

    private ReturnCode handleException(Throwable e) {
        ErrorCode errorCode = errorCodeFactory.apply(e);
        log(errorCode, e);
        return errorCode.getCode();
    }

    private void log(ErrorCode code, Throwable e) {
        logger(code).accept(code.getMessage(), e);
    }

    private BiConsumer<String, Throwable> logger(ErrorCode code) {
        switch (code.getSeverity()) {
            case INFO:
                return logger(code, (msg, e) -> LOG.info(msg), LOG::info);
            case WARNING:
                return logger(code, (msg, e) -> LOG.warn(msg), LOG::warn);
            default:
                return logger(code, LOG::error, LOG::error);
        }
    }

    private BiConsumer<String, Throwable> logger(ErrorCode code, BiConsumer<String, Throwable> logAction, BiConsumer<String, Throwable> debugAction) {
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
