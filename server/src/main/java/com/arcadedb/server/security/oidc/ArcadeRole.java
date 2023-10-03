package com.arcadedb.server.security.oidc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.arcadedb.server.security.oidc.role.CRUDPermission;
import com.arcadedb.server.security.oidc.role.DatabaseAdminRole;
import com.arcadedb.server.security.oidc.role.RoleType;
import com.arcadedb.server.security.oidc.role.ServerAdminRole;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * ArcadeRole is a representation of a role in ArcadeDB in the process of
 * converting between Keycloak and ArcadeDB.
 */
@Data
@NoArgsConstructor
@Slf4j
public class ArcadeRole {
    /** Characters to use to breakup a role string into discrete components */
    public static final String PERMISSION_DELIMITER = "__";

    /**
     * Keycloak role prefix that signifies it is for arcade data access enforcement
     */
    public static final String ROLE_PREFIX = "arcade" + PERMISSION_DELIMITER;
    public static final String ALL_WILDCARD = "*";

    /*
     * The following markers are used to indicate the following role components, and
     * lessen the likelihood of
     * awkward database and table names escaping the delimiter.
     */
    /**
     * Permission delimited text after this marker will contain the database name
     * the role applies to
     */
    public static final String DATABASE_MARKER = "d-";
    /**
     * Permission delimited text after this marker will contain the table regex the
     * role applies to
     */
    public static final String TABLE_MARKER = "t-";
    /**
     * Permission delimited text after this marker will contain the CRUD permissions
     * the role applies to
     */
    public static final String PERMISSION_MARKER = "p-";

    private String name;
    private RoleType roleType;
    private String database;
    private String tableRegex;
    private int readTimeout = -1;
    private int resultSetLimit = -1;

    private DatabaseAdminRole databaseAdminRole;
    private ServerAdminRole serverAdminRole;
    private List<CRUDPermission> crudPermissions = new ArrayList<>(0);

    /**
     * Create new user type arcade role for data access enforcement
     * @param roleType
     * @param database
     * @param tableRegex
     * @param crudPermissions
     */
    public ArcadeRole(RoleType roleType, String database, String tableRegex, List<CRUDPermission> crudPermissions) {
        this.roleType = roleType;
        this.database = database;
        this.tableRegex = tableRegex;
        this.crudPermissions = crudPermissions;
    }

    /**
     * Create new database admin type arcade role for data access enforcement
     * @param roleType
     * @param database
     * @param databaseAdminRole
     */ 
    public ArcadeRole(RoleType roleType, String database, DatabaseAdminRole databaseAdminRole) {
        this.roleType = roleType;
        this.database = database;
        this.databaseAdminRole = databaseAdminRole;
    }

    /**
     * check for non empty parts after marker. consolidate all stream checks to
     * single method that confirms count == 1 and part is not empty after marker
     */
    private static boolean validateRolePart(String keycloakRole, String marker) {
        return Arrays.stream(keycloakRole.split(PERMISSION_DELIMITER))
                .filter(part -> part.startsWith(marker) && part.length() > marker.length())
                .count() == 1;
    }

    /**
     * Validates that the keycloak user role is constructed correctly with parsible
     * components and no duplicates
     */
    private static boolean isValidKeycloakUserRole(String keycloakRole) {
        boolean containsDatabaseMarker = validateRolePart(keycloakRole, DATABASE_MARKER);
        boolean containsTableMarker = validateRolePart(keycloakRole, TABLE_MARKER);
        boolean containsPermissionMarker = validateRolePart(keycloakRole, PERMISSION_MARKER);
        boolean containsRightNumberOfParts = keycloakRole.split(PERMISSION_DELIMITER).length == 5;
        return isArcadeRole(keycloakRole) && containsDatabaseMarker && containsTableMarker && containsPermissionMarker
                && containsRightNumberOfParts;
    }

    /**
     * Validates that the keycloak user role is constructed correctly with parsible
     * components and no duplicates
     */
    private static boolean isValidKeycloakDatabaseAdminRole(String keycloakRole) {
        boolean containsDatabaseMarker = validateRolePart(keycloakRole, DATABASE_MARKER);
        boolean containsRightNumberOfParts = keycloakRole.split(PERMISSION_DELIMITER).length == 4;
        return isArcadeRole(keycloakRole) && containsDatabaseMarker && containsRightNumberOfParts;
    }

    private static boolean isValidKeycloakServerAdminRole(String role) {
        boolean containsRightNumberOfParts = role.split(PERMISSION_DELIMITER).length == 3;
        return isArcadeRole(role) && containsRightNumberOfParts;
    }

    /**
     * Parse JWT role. Has format of
     * 
     * [arcade prefix]__[role type]__[permission args...]
     * 
     * [arcade prefix]__sa__[sa role]
     * [arcade prefix]__dba__d-[database]__[dba role]
     * [arcade prefix]__user__d-[database]__t-[table regex]__p-[crud]
     * 
     */
    public static ArcadeRole valueOf(String role) {
        if (isArcadeRole(role)) {
            ArcadeRole arcadeRole = new ArcadeRole();
            arcadeRole.name = role;
            arcadeRole.roleType = arcadeRole.getRoleTypeFromString(role);
            //log.info("role type: {}", arcadeRole.roleType);

            if (arcadeRole.roleType == null) {
                return null;
            }

            switch (arcadeRole.roleType) {
                case USER:
                    if (isValidKeycloakUserRole(role)) {
                        String[] parts = role.split(PERMISSION_DELIMITER);
                        for (String part : parts) {
                            if (part.startsWith(DATABASE_MARKER)) {
                                arcadeRole.database = part.substring(2);
                            } else if (part.startsWith(TABLE_MARKER)) {
                                arcadeRole.tableRegex = part.substring(2);
                            } else if (part.startsWith(PERMISSION_MARKER)) {
                                arcadeRole.crudPermissions = part.substring(2)
                                        .chars()
                                        .mapToObj(c -> (char) c)
                                        .map(c -> CRUDPermission.fromKeycloakPermissionAbbreviation(String.valueOf(c)))
                                        .collect(Collectors.toList());
                            }
                        }
                    } else {
                        log.warn("invalid arcade role assigned to user: {}", role);
                        return null;
                    }
                    break;
                case DATABASE_ADMIN:
                    // TODO scope to database
                    if (isValidKeycloakDatabaseAdminRole(role)) {
                        arcadeRole.database = role.split(PERMISSION_DELIMITER)[2].substring(DATABASE_MARKER.length());
                        String dbaRoleName = role.split(PERMISSION_DELIMITER)[3];
                        arcadeRole.databaseAdminRole = DatabaseAdminRole.fromKeycloakName(dbaRoleName);
                    } else {
                        log.warn("Invalid keyclaoak database admin role assigned to user: {}", role);
                    }

                    if (arcadeRole.databaseAdminRole == null) {
                        log.warn("invalid database admin arcade role assigned to user: {}", role);
                        return null;
                    }
                    break;
                case SERVER_ADMIN:
                    if (isValidKeycloakServerAdminRole(role)) {
                        String saRoleName = role.split(PERMISSION_DELIMITER)[2];
                        arcadeRole.serverAdminRole = ServerAdminRole.fromKeycloakName(saRoleName);
                    }
                    if (arcadeRole.serverAdminRole == null) {
                        log.warn("invalid server admin arcade role assigned to user: {}", role);
                        return null;
                    }
                    break;
                default:
                    log.warn("invalid arcade role assigned to user: {}", role);
                    return null;
            }
            return arcadeRole;
        }
        return null;
    }

    public static boolean isArcadeRole(String role) {
        return role.startsWith(ROLE_PREFIX);
    }

    private RoleType getRoleTypeFromString(String role) {
        String prefixRemoved = role.substring((ROLE_PREFIX).length());
        String roleString = prefixRemoved.substring(0, prefixRemoved.indexOf(PERMISSION_DELIMITER));
        return RoleType.fromKeycloakName(roleString);
    }

    private static boolean notNullOrEmpty(String s) {
        return s != null && !s.isEmpty();
    }

    /** 
     * Converts the ArcadeRole to a corresponding Keycloak role name.
     */
    public String getKeycloakRoleName() {
        String result = null;
        StringBuilder sb = new StringBuilder();
        sb.append(ROLE_PREFIX);
        sb.append(roleType.getKeycloakName());
        sb.append(PERMISSION_DELIMITER);

        switch (roleType) {
            case USER:
                if (notNullOrEmpty(database) && notNullOrEmpty(tableRegex) && crudPermissions != null
                        && !crudPermissions.isEmpty()) {
                    sb.append(DATABASE_MARKER);
                    sb.append(database);
                    sb.append(PERMISSION_DELIMITER);
                    sb.append(TABLE_MARKER);
                    sb.append(tableRegex);
                    sb.append(PERMISSION_DELIMITER);
                    sb.append(PERMISSION_MARKER);
                    sb.append(crudPermissions.stream().map(p -> p.getKeycloakPermissionAbbreviation())
                            .collect(Collectors.joining()));
                    // TODO result set time and limit
                    result = sb.toString();
                }
                break;
            case DATABASE_ADMIN:
                if (notNullOrEmpty(database) && databaseAdminRole != null) {
                    sb.append(DATABASE_MARKER);
                    sb.append(database);
                    sb.append(PERMISSION_DELIMITER);
                    sb.append(databaseAdminRole.getKeycloakName());
                    result = sb.toString();
                }
                break;
            case SERVER_ADMIN:
                if (serverAdminRole != null) {
                    sb.append(serverAdminRole.getKeycloakName());
                    result = sb.toString();
                }
                break;
            default:
                break;
        }

        return result;
    }

    public List<String> getCrudPermissionsAsArcadeNames() {
        return this.crudPermissions.stream().map(p -> p.getArcadeName()).collect(Collectors.toList());
    }
}
