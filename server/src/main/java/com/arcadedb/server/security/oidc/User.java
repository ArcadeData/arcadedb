package com.arcadedb.server.security.oidc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class User {

    private String name;
    private String password;
    @JsonProperty("databases")
    private Map<String, List<String>> databaseGroups = new HashMap<>();

    public User(String name, Map<String, List<String>> databaseGroups) {
        this.name = name;
        this.databaseGroups = databaseGroups;
    }
}
