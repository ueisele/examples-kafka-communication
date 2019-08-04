package net.uweeisele.examples.kafka.transformer;

import java.util.Objects;
import java.util.Set;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toSet;

public class ReturnCode {

    private final int code;

    ReturnCode(int code) {
        this.code = code;
    }

    public int get() {
        return code;
    }

    @Override
    public String toString() {
        return "ReturnCode{" +
                "code=" + code +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReturnCode that = (ReturnCode) o;
        return code == that.code;
    }

    @Override
    public int hashCode() {
        return Objects.hash(code);
    }

    static ReturnCode success() {
        return new Success();
    }

    public static ReturnCode publicCode(int code) {
        return new Public(code);
    }

    static ReturnCode internalCode(int code) {
        return new Internal(code);
    }

    static class Success extends ReturnCode {

        private static final int SUCCESS = 0;

        public Success() {
            super(SUCCESS);
        }
    }

    public static class Public extends ReturnCode {

        private static final Set<Integer> PUBLIC_RANGE = IntStream.rangeClosed(1, 127).boxed().collect(toSet());

        public Public(int code) {
            super(ensurePublic(code));
        }

        private static int ensurePublic(int code) {
            if (!PUBLIC_RANGE.contains(code)) {
                throw new IllegalArgumentException("Code is not within the public range!");
            }
            return code;
        }
    }

    static class Internal extends ReturnCode {

        private static final Set<Integer> INTERNAL_RANGE = IntStream.rangeClosed(128, 255).boxed().collect(toSet());

        public Internal(int code) {
            super(ensureInternal(code));
        }

        private static int ensureInternal(int code) {
            if (!INTERNAL_RANGE.contains(code)) {
                throw new IllegalArgumentException("Code is not within the public range!");
            }
            return code;
        }
    }
}
