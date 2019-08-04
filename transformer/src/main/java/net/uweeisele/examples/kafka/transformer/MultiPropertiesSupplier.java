package net.uweeisele.examples.kafka.transformer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class MultiPropertiesSupplier implements Supplier<Properties> {

    private List<Supplier<Properties>> suppliers;

    public MultiPropertiesSupplier() {
        this(new ArrayList<>());
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
