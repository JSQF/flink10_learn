package flink.types;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.types.Row;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-06-18
 * @Time 15:54
 */
public class RowRecord extends Row implements GenericRecord, Comparable<GenericData.Record>{
    /**
     * Create a new Row instance.
     *
     * @param arity The number of fields in the Row
     */
    public RowRecord(int arity) {
        super(arity);
    }

    @Override
    public int compareTo(GenericData.Record o) {
        return 0;
    }

    @Override
    public void put(String key, Object v) {

    }

    @Override
    public Object get(String key) {
        return null;
    }

    @Override
    public void put(int i, Object v) {
        super.setField(i, v);
    }

    @Override
    public Object get(int i) {
        return super.getField(i);
    }

    @Override
    public Schema getSchema() {
        return null;
    }
}
