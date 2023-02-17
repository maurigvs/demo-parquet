package br.com.maurigvs.parquet.service;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import br.com.maurigvs.parquet.model.User;

@Service
public class CsvService {

    @Value("${output.directoryPath}")
    private String outputDirectoryPath;

    public void save(List<User> userList) {

        List<String[]> dataLines = new ArrayList<>();

        for (User user : userList){
            String[] line = {user.getUsername(), String.valueOf(user.isActive())};
            dataLines.add(line);
        }

        File csvOutputFile = new File(outputDirectoryPath + "/user-list.csv");
        try (PrintWriter pw = new PrintWriter(csvOutputFile)) {
            dataLines.stream()
                    .map(this::convertToCSV)
                    .forEach(pw::println);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private String convertToCSV(String[] data) {
        return Stream.of(data)
                .map(this::escapeSpecialCharacters)
                .collect(Collectors.joining(","));
    }

    private String escapeSpecialCharacters(String data) {
        String escapedData = data.replaceAll("\\R", " ");
        if (data.contains(",") || data.contains("\"") || data.contains("'")) {
            data = data.replace("\"", "\"\"");
            escapedData = "\"" + data + "\"";
        }
        return escapedData;
    }
}
