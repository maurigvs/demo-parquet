package br.com.maurigvs.demoparquet;

import java.io.IOException;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import br.com.maurigvs.demoparquet.model.User;

@RestController
@RequestMapping("/parquet")
public class ParquetController {

    @Autowired
    ParquetService parquetService;

    @PostMapping
    @ResponseBody
    public ResponseEntity<String> postUser(@RequestBody List<User> userList){
        try {
            parquetService.save(userList);
        } catch (IOException ex) {
            return ResponseEntity.internalServerError().body(ex.getMessage());
        }
        return ResponseEntity.ok("users created: " + userList);
    }
}