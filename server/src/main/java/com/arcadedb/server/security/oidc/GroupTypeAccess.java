package com.arcadedb.server.security.oidc;

import java.util.ArrayList;
import java.util.List;

import com.arcadedb.server.security.oidc.role.CRUDPermission;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Helper class to support serialization/deserialization of arcade ACL config
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class GroupTypeAccess {
    List<String> access = new ArrayList<>();
}
