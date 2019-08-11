package net.uweeisele.examples.kafka.sequence;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.ArgumentType;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static java.lang.String.format;

public class PropertiesFileArgumentType implements ArgumentType<Properties> {

    @Override
    public Properties convert(ArgumentParser parser, Argument arg, String value) throws ArgumentParserException {
        Properties properties = new Properties();
        try(InputStream in = new FileInputStream(value)) {
            properties.load(in);
        } catch (FileNotFoundException e) {
            throw new ArgumentParserException(format("Properties file %s does not exist.", value), e, parser, arg);
        } catch (IOException e) {
            throw new ArgumentParserException(format("Properties file %s could not be read.", value), e, parser, arg);
        }
        return properties;
    }
}
