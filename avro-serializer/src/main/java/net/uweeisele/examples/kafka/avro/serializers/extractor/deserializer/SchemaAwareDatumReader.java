package net.uweeisele.examples.kafka.avro.serializers.extractor.deserializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

public interface SchemaAwareDatumReader<D> extends DatumReader<D> {

    Schema getSchema();

    Schema getExpected();

    void setExpected(Schema reader);

    class Specific<D extends SpecificRecord> extends SpecificDatumReader<D> implements SchemaAwareDatumReader<D>  {
        public Specific() {
            super();
        }
        public Specific(Class<D> c) {
            super(c);
        }
        public Specific(Schema schema) {
            super(schema);
        }
        public Specific(Schema writer, Schema reader) {
            super(writer, reader);
        }
        public Specific(Schema writer, Schema reader, SpecificData data) {
            super(writer, reader, data);
        }
        public Specific(SpecificData data) {
            super(data);
        }

        @Override
        public String toString() {
            return "SpecificDatumReader";
        }
    }

    class Generic extends GenericDatumReader<GenericRecord> implements SchemaAwareDatumReader<GenericRecord>  {
        public Generic() {
           super();
        }
        public Generic(Schema schema) {
            super(schema);
        }
        public Generic(Schema writer, Schema reader) {
            super(writer, reader);
        }
        public Generic(Schema writer, Schema reader, GenericData data) {
            super(writer, reader, data);
        }
        public Generic(GenericData data) {
            super(data);
        }
        @Override
        public String toString() {
            return "GenericDatumReader";
        }
    }

}
