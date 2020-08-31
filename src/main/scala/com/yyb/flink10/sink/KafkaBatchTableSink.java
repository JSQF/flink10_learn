package com.yyb.flink10.sink;

import com.yyb.flink10.OutputFormat.KafkaOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-07-30
 * @Time 13:36
 */
public class KafkaBatchTableSink implements BatchTableSink<String> {

    private final KafkaOutputFormat kafkaOutputFormat;
    private String[] fieldNames = new String[]{"value"};
    private TypeInformation[] fieldTypes = new TypeInformation[]{TypeInformation.of(String.class)};

    public KafkaBatchTableSink(KafkaOutputFormat kafkaOutputFormat){
        this.kafkaOutputFormat = kafkaOutputFormat;
    }


    public void emitDataSet(DataSet<String> dataSet) {
        dataSet.output(kafkaOutputFormat);
    }

    @Override
    public TableSink<String> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        return null;
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
    }

    @Override
    public TypeInformation<String> getOutputType() {
        return TypeInformation.of(String.class);
    }

    @Override
    public DataSink<?> consumeDataSet(DataSet<String> dataSet) {
        return dataSet.output(kafkaOutputFormat);
    }
}
