package br.com.maurigvs.parquet.utiils.reader;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import br.com.maurigvs.parquet.entities.User;

public class UserReadSupport extends ReadSupport<User> {

    @Override
    public RecordMaterializer<User> prepareForRead(
            Configuration configuration, Map<String, String> map,
            MessageType messageType, ReadContext readContext) {
        return null;
    }
}
