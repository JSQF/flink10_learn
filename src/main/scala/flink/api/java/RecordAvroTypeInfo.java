package flink.api.java;

import org.apache.avro.generic.GenericData;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-06-29
 * @Time 11:21
 */
public class RecordAvroTypeInfo extends TypeInformation<GenericData.Record> {
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
    public Class<GenericData.Record> getTypeClass() {
        return null;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<GenericData.Record> createSerializer(ExecutionConfig config) {
        return null;
    }

    @Override
    public String toString() {
        return null;
    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean canEqual(Object obj) {
        return false;
    }
}
