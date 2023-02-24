package br.com.maurigvs.parquet.service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import br.com.maurigvs.parquet.entities.User;
import br.com.maurigvs.parquet.utiils.UserParquetWriter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class ParquetService {

    @Value("${schema.filePath}")
    private String schemaFilePath;

    @Value("${output.directoryPath}")
    private String outputDirectoryPath;

    public void postUsersToFile(List<User> userList) throws Exception {

        userList.forEach(u -> u.setId(UUID.randomUUID()));

        List<List<String>> userData = getUserData(userList);
        MessageType userSchema = getUserSchema(schemaFilePath);
        UserParquetWriter writer = getParquetWriter(userSchema);

        for (List<String> column : userData)
            writer.write(column);
        writer.close();
    }

    private UserParquetWriter getParquetWriter(MessageType schema) throws Exception {

        String outputPath = outputDirectoryPath + "/users.parquet";
        File outputFile = new File(outputPath);
        Path path = new Path(outputFile.toURI().toString());
        return new UserParquetWriter(path, schema, false, CompressionCodecName.SNAPPY);
    }

    private MessageType getUserSchema(String schemaFilePath) throws IOException {

        File resource = new File(schemaFilePath);
        String rawSchema = new String(Files.readAllBytes(resource.toPath()));
        return MessageTypeParser.parseMessageType(rawSchema);
    }

    private List<List<String>> getUserData(List<User> userList) {
        List<List<String>> userData = new ArrayList<>();
        for (User user : userList) {
            List<String> item = new ArrayList<>();
            item.add(user.getId().toString());
            item.add(user.getUsername());
            item.add(String.valueOf(user.isActive()));
            userData.add(item);
        }
        return userData;
    }
}
