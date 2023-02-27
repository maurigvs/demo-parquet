package br.com.maurigvs.parquet.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import br.com.maurigvs.parquet.model.User;
import br.com.maurigvs.parquet.service.UserService;

@RestController
@RequestMapping("/users")
public class UserController {

    @Autowired
    UserService userService;

    @PostMapping
    public ResponseEntity<String> postUsers(@RequestBody List<User> userList) throws Exception {
        userService.saveUsers(userList);
        return ResponseEntity.noContent().build();
    }
}
