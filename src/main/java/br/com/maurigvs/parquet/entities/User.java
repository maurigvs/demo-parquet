package br.com.maurigvs.parquet.entities;

import java.util.UUID;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class User {

    private UUID id;
    private String username;
    private boolean active;
}
