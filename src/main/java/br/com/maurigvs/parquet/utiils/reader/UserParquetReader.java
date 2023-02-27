package br.com.maurigvs.parquet.utiils.reader;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;

import br.com.maurigvs.parquet.entities.User;

public class UserParquetReader extends ParquetReader<User> {

    public UserParquetReader(Path file, ReadSupport<User> readSupport) throws IOException {
        super(file, readSupport);
    }
}
