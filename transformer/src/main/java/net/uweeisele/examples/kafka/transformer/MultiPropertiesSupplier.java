package net.uweeisele.examples.kafka.transformer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class MultiPropertiesSupplier implements Supplier<Properties> {

    private List<Supplier<Properties>> suppliers;

    public MultiPropertiesSupplier() {
        this(new ArrayList<>());
    }

    public MultiPropertiesSupplier(Properties... properties) {
        this(Arrays.stream(properties).map(p -> ((Supplier<Properties>)() -> p)).collect(toList()));
    }

    public MultiPropertiesSupplier(Supplier<Properties>... suppliers) {
        this(new ArrayList<>(asList(suppliers)));
    }

    public MultiPropertiesSupplier(List<Supplier<Properties>> suppliers) {
        this.suppliers = suppliers;
    }

    @Override
    public Properties get() {
        return suppliers.stream().map(Supplier::get).collect(Properties::new, Properties::putAll, Properties::putAll);
    }

    public MultiPropertiesSupplier addProperties(Properties properties) {
        requireNonNull(properties);
        return addPropertiesSupplier(() -> properties);
    }

    public MultiPropertiesSupplier addPropertiesSupplier(Supplier<Properties> supplier) {
        suppliers.add(requireNonNull(supplier));
        return this;
    }
}
