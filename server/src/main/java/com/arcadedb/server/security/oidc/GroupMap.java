package com.arcadedb.server.security.oidc;

import java.util.HashMap;
import java.util.Map;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Map of group names to group objects to support serialization/deserialization of arcade ACL config
 */
@Data
@NoArgsConstructor
public class GroupMap {
    private Map<String, Group> groups = new HashMap<>();
}
