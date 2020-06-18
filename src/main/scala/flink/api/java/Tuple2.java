package flink.api.java;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-06-18
 * @Time 14:07
 */
public class Tuple2<T0, T1> extends org.apache.flink.api.java.tuple.Tuple2<T0, T1> implements GenericRecord, Comparable<GenericData.Record>{
    private  Object[] values = new Object[2];
    private  Schema schema = null;
    public Tuple2(){
        super();
    }
    public Tuple2(Schema schema) {
        super();
        if (schema == null || !Schema.Type.RECORD.equals(schema.getType()))
            throw new AvroRuntimeException("Not a record schema: "+schema);
        this.schema = schema;
        this.values = new Object[schema.getFields().size()];
    }

    public Tuple2(Schema schema, T0 value0, T1 value1) {
        super(value0, value1);
        if (schema == null || !Schema.Type.RECORD.equals(schema.getType()))
            throw new AvroRuntimeException("Not a record schema: "+schema);
        this.schema = schema;
        this.values = new Object[2];
        this.values[0] = value0;
        this.values[1] = value1;
    }

    public Tuple2(T0 value0, T1 value1) {
        super(value0, value1);
        this.values = new Object[2];
        this.values[0] = value0;
        this.values[1] = value1;
    }

    @Override
    public int compareTo(GenericData.Record o) {
        return GenericData.get().compare(this, o, schema);
    }

    @Override
    public void put(String key, Object v) {
        Schema.Field field = schema.getField(key);
        if (field == null)
            throw new AvroRuntimeException("Not a valid schema field: "+key);

        values[field.pos()] = v;
    }

    @Override
    public Object get(String key) {
        Schema.Field field = schema.getField(key);
        if (field == null) return null;
        return values[field.pos()];
    }

    @Override
    public void put(int i, Object v) {
        values[i] = v;
    }

    @Override
    public Object get(int i) {
        return values[i];
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public <T> void setField(T value, int pos){
        super.setField(value, pos);
        this.values[pos] = value;
    }

    @Override
    public void setFields(T0 value0, T1 value1){
        super.setFields(value0, value1);
        this.values[0] = value0;
        this.values[1] = value1;
    }

    public static <T0, T1> org.apache.flink.api.java.tuple.Tuple2<T0, T1> of(T0 value0, T1 value1) {
        return new Tuple2<T0, T1>(value0,
                value1);
    }
}
