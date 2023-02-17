package br.com.maurigvs.demoparquet;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import br.com.maurigvs.demoparquet.model.User;

@Service
public class ParquetService {

    @Value("${schema.filePath}")
    private String schemaFilePath;

    @Value("${output.directoryPath}")
    private String outputDirectoryPath;

    public void save(List<User> userList) throws IOException {

        List<List<String>> columns = getDataForFile(userList);
        MessageType schema = getSchemaForParquetFile();
        CustomParquetWriter writer = getParquetWriter(schema);

        for (List<String> column : columns) {
            System.out.println("Writing line: " + Arrays.toString(column.toArray()));
            writer.write(column);
        }

        System.out.println("Finished writing Parquet file.");

        writer.close();
    }

    private CustomParquetWriter getParquetWriter(MessageType schema) throws IOException {
        String outputFilePath = outputDirectoryPath + "/" + System.currentTimeMillis() + ".parquet";
        File outputParquetFile = new File(outputFilePath);
        Path path = new Path(outputParquetFile.toURI().toString());
        return new CustomParquetWriter(
                path, schema, false, CompressionCodecName.SNAPPY
        );
    }

    private MessageType getSchemaForParquetFile() throws IOException {
        File resource = new File(schemaFilePath);
        String rawSchema = new String(Files.readAllBytes(resource.toPath()));
        return MessageTypeParser.parseMessageType(rawSchema);
    }

    private List<List<String>> getDataForFile(List<User> userList) {

        List<List<String>> data = new ArrayList<>();
        int counter = 0;

        for (User user : userList) {
            counter++;
            List<String> fileItem = new ArrayList<>();
            fileItem.add(String.valueOf(counter));
            fileItem.add(user.getUsername());
            fileItem.add(String.valueOf(user.isActive()));
            data.add(fileItem);
        }

        return data;
    }
}
