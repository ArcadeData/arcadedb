package com.arcadedb.server.security.oidc;

import java.util.ArrayList;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KeycloakUser {
    private String username;
    private List<String> roles = new ArrayList<>();
}
