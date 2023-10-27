package com.arcadedb.server.security.oidc.role;

import com.fasterxml.jackson.annotation.JsonValue;

import lombok.Getter;

/**
 * Flags the type of role being defined, ie end user, database admin, server admin
 */
public enum RoleType {
    DATABASE_ADMIN("dba", "dba"),
    USER("user", "user"),
    SERVER_ADMIN("sa","sa"),
    DATA_STEWARD("dataSteward", "dataSteward");

    /**
     * Keyword used in the keycloak role name to look for when knowing what type of role is being defined
     */
    @Getter
    private String keycloakName;

    private String arcadeName;

    RoleType(String keycloakName, String arcadeName) {
        this.keycloakName = keycloakName;
        this.arcadeName = arcadeName;
    }

    @JsonValue
    public String getArcadeName() {
        return arcadeName;
    }

    public static RoleType fromKeycloakName(String name) {
        for (RoleType roleType : RoleType.values()) {
            if (roleType.keycloakName.equalsIgnoreCase(name)) {
                return roleType;
            }
        }
        return null;
    }
}
