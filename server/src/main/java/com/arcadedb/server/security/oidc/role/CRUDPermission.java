package com.arcadedb.server.security.oidc.role;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonValue;

import lombok.Getter;

/**
 * Deliniation of row/document level permissions for CRUD/user operations
 */
public enum CRUDPermission {
    CREATE("C", "createRecord"),
    READ("R", "readRecord"),
    UPDATE("U", "updateRecord"),
    DELETE("D", "deleteRecord");

    // TODO rename this class to generic permission, and add in history and record rollback permissions
    // Requires the history and rollback permissions be stored outside arcade ACLs and in the serverSecurity cache

    /**
     * Single letter abbrivation of the permission, used in the keycloak role name such 
     * that multiple permissions can be assigned with the same role.
     * 
     * IE arcade__user__...__CRD
     */
    @Getter
    private String keycloakPermissionAbbreviation;

    /**
     * Arcade syntax for the permission to be stored in Arcade's user/group ACL configuration
     */
    private String arcadeName;

    CRUDPermission(String keycloakPermissionAbbreviation, String arcadeName) {
        this.keycloakPermissionAbbreviation = keycloakPermissionAbbreviation;
        this.arcadeName = arcadeName;
    }

    @JsonValue
    public String getArcadeName() {
        return arcadeName;
    }

    public static CRUDPermission fromKeycloakPermissionAbbreviation(String keycloakPermissionAbbreviation) {
        for (CRUDPermission crud : CRUDPermission.values()) {
            if (crud.keycloakPermissionAbbreviation.equalsIgnoreCase(keycloakPermissionAbbreviation)) {
                return crud;
            }
        }
        return null;
    }

    public static List<CRUDPermission> getAll(){
        return List.of(CRUDPermission.values());
    }
}
