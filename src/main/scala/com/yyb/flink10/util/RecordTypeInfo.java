package com.yyb.flink10.util;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.List;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-06-17
 * @Time 15:15
 */
public class RecordTypeInfo<T> extends CompositeType<T> implements GenericRecord, Comparable<GenericData.Record>  {

//    private final Object[] fields;

    public RecordTypeInfo(Class<T> typeClass, TypeInformation<?>... types){
        super(typeClass);
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

    }

    @Override
    public Object get(int i) {
        return null;
    }

    @Override
    public Schema getSchema() {
        return null;
    }

    @Override
    public void getFlatFields(String fieldExpression, int offset, List result) {

    }

    @Override
    public TypeInformation getTypeAt(String fieldExpression) {
        return null;
    }

    @Override
    public TypeInformation getTypeAt(int pos) {
        return null;
    }

    @Override
    protected TypeComparatorBuilder createTypeComparatorBuilder() {
        return null;
    }

    @Override
    public String[] getFieldNames() {
        return new String[0];
    }

    @Override
    public int getFieldIndex(String fieldName) {
        return 0;
    }

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 0;
    }

    @Override
    public int getTotalFields() {
        return 0;
    }

    @Override
    public TypeSerializer createSerializer(ExecutionConfig config) {
        return null;
    }
}
