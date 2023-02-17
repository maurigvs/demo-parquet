package br.com.maurigvs.parquet.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import br.com.maurigvs.parquet.model.User;
import br.com.maurigvs.parquet.service.CsvService;

@RestController
@RequestMapping("/csv")
public class CsvController {

    @Autowired
    CsvService csvService;

    @PostMapping
    @ResponseBody
    public ResponseEntity<String> postUser(@RequestBody List<User> userList){
        try {
            csvService.save(userList);
        } catch (RuntimeException ex) {
            return ResponseEntity.internalServerError().body(ex.getMessage());
        }
        return ResponseEntity.ok("users created: " + userList);
    }
}