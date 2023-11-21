package com.arcadedb.server.security.oidc.role;

import com.fasterxml.jackson.annotation.JsonValue;

import lombok.Getter;

/**
 * Permission to modify arcade server or deployment level settings or data
 */
public enum ServerAdminRole {
    CREATE_DATABASE(Constants.CREATE_DATABASE, Constants.CREATE_DATABASE),
    DROP_DATABASE(Constants.DROP_DATABASE, Constants.DROP_DATABASE),
    BACKUP_DATABASE(Constants.BACKUP_DATABASE, Constants.BACKUP_DATABASE),
    ALL(Constants.ALL, Constants.ALL);

    @Getter
    private String keycloakName;

    private String arcadeName;

    ServerAdminRole(String keycloakName, String arcadeName) {
        this.keycloakName = keycloakName;
        this.arcadeName = arcadeName;
    }

    @JsonValue
    public String getArcadeName() {
        return arcadeName;
    }

    public static ServerAdminRole fromKeycloakName(String keycloakName) {
        for (ServerAdminRole serverAdminRole : ServerAdminRole.values()) {
            if (serverAdminRole.keycloakName.equalsIgnoreCase(keycloakName)) {
                return serverAdminRole;
            }
        }
        return null;
    }

    public static class Constants {
        public static final String CREATE_DATABASE = "createDatabase";
        public static final String DROP_DATABASE = "dropDatabase";
        public static final String BACKUP_DATABASE = "backupDatabase";
        public static final String ALL = "*";
    }
}
