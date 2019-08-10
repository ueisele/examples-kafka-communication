package net.uweeisele.examples.kafka.transformer;

public enum ShutdownHandler {

    /**
     * Do not handle triggered shutdown
     */
    NONE,

    /**
     * Handle triggered shutdown by shutdown hook
     */
    HOOK,

    /**
     * Handle triggered shutdown by signal handler
     */
    SIGNAL
}
