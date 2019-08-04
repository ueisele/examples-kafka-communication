package net.uweeisele.examples.kafka.transformer;

import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class KafkaStreamsRunner implements Callable<Integer>, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsRunner.class);

    private static final int SUCCESS = 0;
    private static final int UNEXPECTED_EXCEPTION = -1;

    private final Function<Properties, KafkaStreams> kafkaStreamsBuilder;
    private final Supplier<Properties> propertiesSupplier;

    private final Runtime runtime;
    private final Function<Runnable, Thread> shutdownHookThreadFactory;

    private final AtomicBoolean hasStarted = new AtomicBoolean(false);
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private final CountDownLatch shutdownCompletedLatch = new CountDownLatch(1);

    public KafkaStreamsRunner(Function<Properties, KafkaStreams> kafkaStreamsBuilder) {
        this(kafkaStreamsBuilder, Properties::new);
    }

    public KafkaStreamsRunner(Function<Properties, KafkaStreams> kafkaStreamsBuilder, Supplier<Properties> propertiesSupplier) {
        this(kafkaStreamsBuilder, propertiesSupplier, Runtime.getRuntime(), Thread::new);
    }

    public KafkaStreamsRunner(Function<Properties, KafkaStreams> kafkaStreamsBuilder, Supplier<Properties> propertiesSupplier, Runtime runtime, Function<Runnable, Thread> shutdownHookThreadFactory) {
        this.kafkaStreamsBuilder = requireNonNull(kafkaStreamsBuilder);
        this.propertiesSupplier = requireNonNull(propertiesSupplier);
        this.runtime = requireNonNull(runtime);
        this.shutdownHookThreadFactory = requireNonNull(shutdownHookThreadFactory);
    }

    @Override
    public Integer call() throws IllegalStateException {
        assertOnlyCalledOnce();

        registerShutdownHook();

        int returnCode;
        try(KafkaStreams kafkaStreams = kafkaStreamsBuilder.apply(propertiesSupplier.get())) {
            kafkaStreams.start();
            shutdownLatch.await();
            returnCode = SUCCESS;
        } catch (ApplicationException applicationException) {
            returnCode = handleApplicationException(applicationException);
        } catch (InterruptedException interruptedException) {
            returnCode = handleInterruptedException();
        } catch (Exception exception) {
            returnCode = handleUnexpectedException(exception);
        } finally {
            shutdownCompletedLatch.countDown();
        }
        return returnCode;
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

    private int handleApplicationException(ApplicationException applicationException) {
        if (applicationException.isLogException()) {
            LOG.error(applicationException.getLocalizedMessage());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(applicationException.getLocalizedMessage(), applicationException);
        }
        return applicationException.getErrorCode();
    }

    private int handleInterruptedException() {
        LOG.info("Application has been interrupted! Stopping now!");
        Thread.currentThread().interrupt();
        return UNEXPECTED_EXCEPTION;
    }

    private int handleUnexpectedException(Exception exception) {
        LOG.error("Unexpected exception occurred which caused abnormal termination of this application!", exception);
        return UNEXPECTED_EXCEPTION;
    }

}
