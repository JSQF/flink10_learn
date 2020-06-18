package com.yyb.flink10.util;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.reflect.ReflectData;
import org.apache.flink.formats.parquet.ParquetBuilder;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-06-17
 * @Time 14:37
 */
public class ParquetAvroWritersSelf{
    public static ParquetWriterFactory<GenericData.Record> forGenericRecord(Schema schema) {
        final String schemaString = schema.toString();
        final ParquetBuilder<GenericData.Record> builder = (out) -> createAvroParquetWriter(schemaString, GenericData.get(), out);
        return new ParquetWriterFactory<>(builder);
    }

    private static <T> ParquetWriter<T> createAvroParquetWriter(
            String schemaString,
            GenericData dataModel,
            OutputFile out) throws IOException {

        final Schema schema = new Schema.Parser().parse(schemaString);

        return AvroParquetWriter.<T>builder(out)
                .withSchema(schema)
                .withDataModel(dataModel)
                .build();
    }
}
