package br.com.maurigvs.parquet.utiils.writer;

import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

public class UserParquetWriter extends ParquetWriter<List<String>> {

    public UserParquetWriter(Path file, MessageType schema, boolean enabledDictionary, CompressionCodecName codecName) throws Exception {
        super(file, new UserWriteSupport(schema), codecName, DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE, enabledDictionary, false);
    }
}