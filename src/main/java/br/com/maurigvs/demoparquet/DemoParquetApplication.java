package br.com.maurigvs.demoparquet;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DemoParquetApplication implements CommandLineRunner {

    @Value("${schema.filePath}")
    private String schemaFilePath;

    @Value("${output.directoryPath}")
    private String outputDirectoryPath;

    public static void main(String[] args) {
        SpringApplication.run(DemoParquetApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {

        List<List<String>> columns = getDataForFile();
        MessageType schema = getSchemaForParquetFile();
        CustomParquetWriter writer = getParquetWriter(schema);

        for (List<String> column : columns) {
            System.out.println("Writing line: " + column.toArray());
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

    private List<List<String>> getDataForFile() {
        List<List<String>> data = new ArrayList<>();

        List<String> parquetFileItem1 = new ArrayList<>();
        parquetFileItem1.add("1");
        parquetFileItem1.add("Name1");
        parquetFileItem1.add("true");

        List<String> parquetFileItem2 = new ArrayList<>();
        parquetFileItem2.add("2");
        parquetFileItem2.add("Name2");
        parquetFileItem2.add("false");

        data.add(parquetFileItem1);
        data.add(parquetFileItem2);

        return data;
    }
}