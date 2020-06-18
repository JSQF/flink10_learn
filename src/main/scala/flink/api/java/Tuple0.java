package flink.api.java;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-06-18
 * @Time 14:07
 */
public class Tuple0 extends org.apache.flink.api.java.tuple.Tuple0 implements GenericRecord, Comparable<GenericData.Record>{
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

    }

    @Override
    public Object get(int i) {
        return null;
    }

    @Override
    public Schema getSchema() {
        return null;
    }
}
