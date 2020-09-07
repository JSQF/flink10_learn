package com.yyb.flink10.util;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.*;
import java.util.stream.IntStream;

public class SimpleTableSchema implements Serializable {

    private final String[] fieldNames;
    private final DataType[] fieldDataTypes;
    private final Map<String, Integer> fieldNameToIndex;
    private String[] keyFields;

    private SimpleTableSchema(String[] fieldNames, DataType[] fieldDataTypes) {
        this.fieldNames = (String[]) Preconditions.checkNotNull(fieldNames);
        this.fieldDataTypes = (DataType[]) Preconditions.checkNotNull(fieldDataTypes);
        if (fieldNames.length != fieldDataTypes.length) {
            throw new TableException(
                    "Number of field names and field data types must be equal.\nNumber of names is "
                            + fieldNames.length + ", number of data types is "
                            + fieldDataTypes.length + ".\nList of field names: " + Arrays
                            .toString(fieldNames) + "\nList of field data types: " + Arrays
                            .toString(fieldDataTypes));
        } else {
            this.fieldNameToIndex = new HashMap();
            Set<String> duplicateNames = new HashSet();
            Set<String> uniqueNames = new HashSet();

            for (int i = 0; i < fieldNames.length; ++i) {
                Preconditions.checkNotNull(fieldDataTypes[i]);
                String fieldName = (String) Preconditions.checkNotNull(fieldNames[i]);
                this.fieldNameToIndex.put(fieldName, i);
                if (uniqueNames.contains(fieldName)) {
                    duplicateNames.add(fieldName);
                } else {
                    uniqueNames.add(fieldName);
                }
            }

            if (!duplicateNames.isEmpty()) {
                throw new TableException(
                        "Field names must be unique.\nList of duplicate fields: " + duplicateNames
                                .toString() + "\nList of all fields: " + Arrays
                                .toString(fieldNames));
            }
        }
    }

    public SimpleTableSchema copy() {
        return new SimpleTableSchema((String[]) this.fieldNames.clone(),
                (DataType[]) this.fieldDataTypes.clone());
    }
    public DataType[] getFieldDataTypes() {
        return this.fieldDataTypes;
    }

    public Optional<DataType> getFieldDataType(int fieldIndex) {
        return fieldIndex >= 0 && fieldIndex < this.fieldDataTypes.length ? Optional
                .of(this.fieldDataTypes[fieldIndex]) : Optional.empty();
    }

    public Optional<DataType> getFieldDataType(String fieldName) {
        return this.fieldNameToIndex.containsKey(fieldName) ? Optional
                .of(this.fieldDataTypes[((Integer) this.fieldNameToIndex.get(fieldName))
                        .intValue()]) : Optional.empty();
    }

    public int getFieldCount() {
        return this.fieldNames.length;
    }

    public String[] getFieldNames() {
        return this.fieldNames;
    }

    public Optional<String> getFieldName(int fieldIndex) {
        return fieldIndex >= 0 && fieldIndex < this.fieldNames.length ? Optional
                .of(this.fieldNames[fieldIndex]) : Optional.empty();
    }

    public DataType toRowDataType() {
        Field[] fields = (Field[]) IntStream.range(0, this.fieldDataTypes.length).mapToObj((i) -> {
            return DataTypes.FIELD(this.fieldNames[i], this.fieldDataTypes[i]);
        }).toArray((x$0) -> {
            return new Field[x$0];
        });
        return DataTypes.ROW(fields);
    }

    /**
     * table connector API only support decimal(38,18)
     */
    public void updateDecimalType() {
        if (fieldDataTypes != null) {
            for (int i = 0; i < fieldDataTypes.length; i++) {
                DataType dataType = fieldDataTypes[i];
                if (dataType.getLogicalType() instanceof DecimalType) {
                    fieldDataTypes[i] = DataTypes.DECIMAL(38, 18);
                }
            }
        }
    }


    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("root\n");

        for (int i = 0; i < this.fieldNames.length; ++i) {
            sb.append(" |-- ").append(this.fieldNames[i]).append(": ")
                    .append(this.fieldDataTypes[i]).append('\n');
        }

        return sb.toString();
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            SimpleTableSchema schema = (SimpleTableSchema) o;
            return Arrays.equals(this.fieldNames, schema.fieldNames) && Arrays
                    .equals(this.fieldDataTypes, schema.fieldDataTypes);
        } else {
            return false;
        }
    }

    public int hashCode() {
        int result = Arrays.hashCode(this.fieldNames);
        result = 31 * result + Arrays.hashCode(this.fieldDataTypes);
        return result;
    }

    public TableSchema toTableSchema() {
        TableSchema.Builder builder = new TableSchema.Builder();
        builder.fields(this.fieldNames, this.fieldDataTypes);
        if( keyFields != null && keyFields.length > 0 ) {
            builder.primaryKey(keyFields);
        }
        return builder.build();
    }

    public TableSchema toTableSchema(String[] keyFields) {
        TableSchema.Builder builder = new TableSchema.Builder();
        builder.fields(this.fieldNames, this.fieldDataTypes);
        builder.primaryKey(keyFields);
        return builder.build();
    }

    public String[] getKeyFields() {
        return keyFields;
    }

    public void setKeyFields(String[] keyFields) {
        this.keyFields = keyFields;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private List<String> fieldNames = new ArrayList();
        private List<DataType> fieldDataTypes = new ArrayList();

        public Builder() {
        }

        public Builder field(String name, DataType dataType) {
            Preconditions.checkNotNull(name);
            Preconditions.checkNotNull(dataType);
            this.fieldNames.add(name);
            this.fieldDataTypes.add(dataType);
            return this;
        }

        public Builder fields(String[] names, DataType[] dataTypes) {
            Preconditions.checkNotNull(names);
            Preconditions.checkNotNull(dataTypes);
            this.fieldNames.addAll(Arrays.asList(names));
            this.fieldDataTypes.addAll(Arrays.asList(dataTypes));
            return this;
        }

        public SimpleTableSchema build() {
            return new SimpleTableSchema((String[]) this.fieldNames.toArray(new String[0]),
                    (DataType[]) this.fieldDataTypes.toArray(new DataType[0]));
        }
    }
}
