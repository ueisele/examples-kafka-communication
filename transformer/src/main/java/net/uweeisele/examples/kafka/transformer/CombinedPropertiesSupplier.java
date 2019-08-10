package net.uweeisele.examples.kafka.transformer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class CombinedPropertiesSupplier implements Supplier<Properties> {

    private List<Supplier<Properties>> suppliers;

    public CombinedPropertiesSupplier() {
        this(new ArrayList<>());
    }

    public CombinedPropertiesSupplier(List<Supplier<Properties>> suppliers) {
        this.suppliers = suppliers;
    }

    @Override
    public Properties get() {
        return suppliers.stream().map(Supplier::get).collect(Properties::new, Properties::putAll, Properties::putAll);
    }

    public CombinedPropertiesSupplier addProperties(Properties properties) {
        requireNonNull(properties);
        return addPropertiesSupplier(() -> properties);
    }

    public CombinedPropertiesSupplier addPropertiesSupplier(Supplier<Properties> supplier) {
        suppliers.add(requireNonNull(supplier));
        return this;
    }
}
