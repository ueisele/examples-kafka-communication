package net.uweeisele.examples.kafka.serde.avro.protocol.matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class ExactMatchPatternBuilder {

    private final List<String> strings = new ArrayList<>();

    public Pattern build() {
        return Pattern.compile(strings.stream()
                .map(Pattern::quote)
                .map(ExactMatchPatternBuilder::wrapInGroup)
                .collect(joining("|")));
    }

    private static String wrapInGroup(String regex) {
        return format("(?:%s)", regex);
    }

    public ExactMatchPatternBuilder withMatchExact(String string) {
        strings.add(string);
        return this;
    }

    public static void main(String[] args) {
        //Pattern pattern = Pattern.compile("^ue\\.kafka\\.avro\\.v1\\.MyEvent$");
        //Pattern pattern = Pattern.compile("(?:\\Que.kafka.avro.v1.MyEvent\\E)");
        Pattern pattern = new ExactMatchPatternBuilder()
                .withMatchExact("ue.kafka.avro.v1.MyEvent")
                .withMatchExact("ue.kafka.avro.v2.MyEvent")
                .withMatchExact("ue.kafka.avro.v3.MyEvent")
                .withMatchExact("ue.kafka.avro.v4.MyEvent")
                .withMatchExact("ue.kafka.avro.v5.MyEvent")
                .build();
        boolean hasAccepted = pattern.pattern().contains("(?<accepted>");
        String input = "ue.kafka.avro.v5.MyEvent";
        long duration = 0;
        int rounds = 100;
        for (int r=0;r<rounds;r++) {
            long start = System.currentTimeMillis();
            for (int i = 0; i < 1000000; i++) {
                check(pattern, input, hasAccepted);
            }
            long end = System.currentTimeMillis();
            duration += (end - start);
            System.out.println(format("Duration: %d ms", end - start));
        }
        System.out.println(format("Average: %d ms", duration / rounds));
        System.out.println(check(pattern, input, hasAccepted));
        System.out.println(pattern.pattern());
    }

    public static String check(Pattern pattern, String input, boolean hasAccepted) {
        Matcher matcher = pattern.matcher(input);
        if(matcher.matches()) {

            if (hasAccepted) {
                String accepted = matcher.group("accepted");
                if (accepted != null) {
                    return accepted;
                }
            }

            //String known = matcher.group("known");
            //if (known != null) {
            //    return known;
            //}
            return "known";
        }
        return "unknown";
    }
}
