package br.com.maurigvs.parquet.service;

import java.util.List;
import java.util.UUID;

import org.apache.parquet.schema.MessageType;
import org.springframework.stereotype.Service;

import br.com.maurigvs.parquet.model.User;
import br.com.maurigvs.parquet.service.writer.CustomParquetWriter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class UserService extends ParquetService {

    public void saveUsers(List<User> userList) throws Exception {
        userList.forEach(u -> u.setId(UUID.randomUUID()));

        List<List<String>> userData = getUserData(userList);
        MessageType userSchema = getUserSchema(schemaFilePath);
        CustomParquetWriter writer = getParquetWriter(userSchema);

        for (List<String> column : userData)
            writer.write(column);
        writer.close();
    }
}