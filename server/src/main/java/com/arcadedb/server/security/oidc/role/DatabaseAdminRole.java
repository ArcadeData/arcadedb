package com.arcadedb.server.security.oidc.role;

import com.fasterxml.jackson.annotation.JsonValue;

import lombok.Getter;

/**
 * Permission types for modifying individual databases on an arcade deployment.
 */
public enum DatabaseAdminRole {

    SECURITY(Constants.UPDATE_SECURITY, Constants.UPDATE_SECURITY),
    SCHEMA(Constants.UPDATE_SCHEMA, Constants.UPDATE_SCHEMA),
    DATABASE_SETTINGS(Constants.UPDATE_DATABASE_SETTINGS, Constants.UPDATE_DATABASE_SETTINGS),
    ALL("*", "*");

    @Getter
    private String keycloakName;

    private String arcadeName;

    DatabaseAdminRole(String keycloakName, String arcadeName) {
        this.keycloakName = keycloakName;
        this.arcadeName = arcadeName;
    }

    @JsonValue
    public String getArcadeName() {
        return arcadeName;
    }

    public static DatabaseAdminRole fromKeycloakName(String keycloakName) {
        for (DatabaseAdminRole databaseAdminRole : DatabaseAdminRole.values()) {
            if (databaseAdminRole.keycloakName.equalsIgnoreCase(keycloakName)) {
                return databaseAdminRole;
            }
        }
        return null;
    }

    public static class Constants {
        /** Indicates the user is authorized to manage arcade users and groups */
        public static final String UPDATE_SECURITY = "updateSecurity";

        /** Indicates the user is authorized to manage database  schemas, types, and indexes */
        public static final String UPDATE_SCHEMA = "updateSchema";

        public static final String UPDATE_DATABASE_SETTINGS = "updateDatabaseSettings";
    }
}
