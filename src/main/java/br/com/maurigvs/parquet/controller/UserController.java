package br.com.maurigvs.parquet.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import br.com.maurigvs.parquet.entities.User;
import br.com.maurigvs.parquet.service.ParquetService;

@RestController
@RequestMapping("/users")
public class UserController {

    @Autowired
    ParquetService parquetService;

    @PostMapping
    public ResponseEntity<String> postUsers(@RequestBody List<User> userList){
        try {
            parquetService.postUsersToFile(userList);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(e.getMessage());
        }
        return ResponseEntity.noContent().build();
    }
}
