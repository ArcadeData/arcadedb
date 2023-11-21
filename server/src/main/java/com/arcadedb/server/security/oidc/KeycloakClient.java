package com.arcadedb.server.security.oidc;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.DataFabricRestClient;

import lombok.extern.slf4j.Slf4j;

/**
 * Keycloak rest API client, handling login and admin operations.
 * 
 * A solid chunk of this can go away if/when keycloak implements their Token Exchange feature.
 * It is currently experimental and not maintained..... It would
 * allow use to request a token for a user, which would include all effective roles for that user. Instead
 * we need to make multiple requests to get user client and realm roles separately, get the groups the user belongs to,
 * the client and realm roles for each group, and support requests to get the ids of the objects we're working with....

 */
@Slf4j
public class KeycloakClient extends DataFabricRestClient {

    // TODO convert static usage to non static usage
    // TODO cache username -> userid mapping, client name to client id mapping
    // TODO update getter methods to check cache first, and on cache miss call keycloak, and cache response



    private static String getUserId(String username) {
        String url = getBaseKeycloakAdminUrl() + "/users";
        var userResponse = sendAuthenticatedGetAndGetResponse(url);

        if (userResponse != null) {
            JSONArray usersJA = new JSONArray(userResponse);
            for (int i = 0; i < usersJA.length(); i++) {
                var user = usersJA.getJSONObject(i);
                if (user.getString("username").equals(username)) {
                    log.debug("getUserId for user {}; id {}", username, user.getString("id"));
                    return user.getString("id");
                }
            }
        }

        return null;
    }

    public static Map<String, Object> getUserAttributes(String username) {
        String url = getBaseKeycloakAdminUrl() + "/users";
        var userResponse = sendAuthenticatedGetAndGetResponse(url);

        if (userResponse != null) {
            JSONArray usersJA = new JSONArray(userResponse);
            for (int i = 0; i < usersJA.length(); i++) {
                var user = usersJA.getJSONObject(i);
                if (user.getString("username").equals(username) && user.has("attributes")) {
                    return user.getJSONObject("attributes").toMap();
                }
            }
        }

        return null;
    }

    private static String getClientId(String clientName) {
        String url = getBaseKeycloakAdminUrl() + "/clients";
        var userReponse = sendAuthenticatedGetAndGetResponse(url);

        if (userReponse != null) {
            JSONArray ja = new JSONArray(userReponse);
            for (int i = 0; i < ja.length(); i++) {
                var client = ja.getJSONObject(i);
                if (client.getString("clientId").equals(clientName)) {
                    log.debug("getClientId for client {}; id {}", clientName, client.getString("id"));
                    return client.getString("id");
                }
            }
        }

        return null;
    }

    private static String getClientRoleId(String userId, String clientId, String roleName) {
        // get the role id to assign
        String url = String.format("%s/users/%s/role-mappings/clients/%s/available", getBaseKeycloakAdminUrl(),
                userId, clientId);
        var userReponse = sendAuthenticatedGetAndGetResponse(url);

        if (userReponse != null) {
            JSONArray ja = new JSONArray(userReponse);
            for (int i = 0; i < ja.length(); i++) {
                var role = ja.getJSONObject(i);
                if (role.getString("name").equals(roleName)) {
                    log.debug("getClientRoleId for role {}; id {}", roleName, role.getString("id"));
                    return role.getString("id");
                }
            }
        }

        return null;
    }

    public static List<String> getUserClientRoles(String username) {
        List<String> roles = new ArrayList<>();

        String clientId = GlobalConfiguration.KEYCLOAK_CLIENT_ID.getValueAsString();

        String userId = getUserId(username);
        if (userId != null) {
            // get user roles
            // http://localhost/auth/admin/realms/data-fabric/users/c8019daf-b6a0-410a-a81a-f91530f1ae36/role-mappings/clients/c4892c81-0c07-4283-b269-2339fb7472ca/available
            String url = String.format("%s/users/%s/role-mappings", getBaseKeycloakAdminUrl(), userId);

            var rolesResponse = sendAuthenticatedGetAndGetResponse(url);
            log.debug("getUserClientRoles {}", rolesResponse);
            if (rolesResponse != null) {
                JSONObject rolesJO = new JSONObject(rolesResponse);

                if (rolesJO.has("clientMappings") && rolesJO.getJSONObject("clientMappings").has(clientId)) {
                    var clientMappings = rolesJO.getJSONObject("clientMappings");
                    var dfBackend = clientMappings.getJSONObject(clientId);
                    var mappings = dfBackend.getJSONArray("mappings");

                    roles = mappings.toList().stream().map(m -> {
                        var jsonObject = (LinkedHashMap<String, Object>) m;
                        return jsonObject.get("name").toString();
                    }).collect(Collectors.toList());
                }
            }

            // get user groups
            List<String> groupIds = getUserGroupIds(userId);
            for (String groupId : groupIds) {
                roles.addAll(getClientRolesForGroup(groupId, clientId));
            }
        }

        return roles;
    }

    public static List<String> getUserGroupIds(String userId) {
        String url = String.format("%s/users/%s/groups", getBaseKeycloakAdminUrl(), userId);
        String response = sendAuthenticatedGetAndGetResponse(url);
        List<String> groupIds = new ArrayList<>();

        if (response != null) {
            JSONArray ja = new JSONArray(response);

            for (int i = 0; i < ja.length(); i++) {
                var role = ja.getJSONObject(i);

                if (role.has("id")) {
                    groupIds.add(role.getString("id"));
                }
            }
        }

        return groupIds;
    }

    public static List<String> getClientRolesForGroup(String groupId, String clientName) {
        List<String> roles = new ArrayList<>();
        String clientId = getClientId(clientName);
        String url = String.format("%s/groups/%s/role-mappings/clients/%s", getBaseKeycloakAdminUrl(), groupId,
                clientId);

        String response = sendAuthenticatedGetAndGetResponse(url);
        if (response != null) {
            JSONArray ja = new JSONArray(response);

            for (int i = 0; i < ja.length(); i++) {
                var role = ja.getJSONObject(i);

                if (role.has("name")) {
                    roles.add(role.getString("name"));
                }
            }
        }

        return roles;
    }

    public static void createRole(String roleName) {
        String clientId = getClientId(GlobalConfiguration.KEYCLOAK_CLIENT_ID.getValueAsString());

        // TODO parameterize below url with config
        String url = String.format("%s/clients/%s/roles", getBaseKeycloakAdminUrl(), clientId);
        JSONObject request = new JSONObject();
        request.put("name", roleName);

        // TODO extend arcade role to generate a human readable description, and
        // reference it here.
        postAuthenticatedAndGetResponse(url, request.toString());
    }

    public static void deleteRole(String roleName) {

    }

    public static void assignRoleToUser(String roleName, String username) {
        // get the id of the user to assign the role to
        String userId = getUserId(username);
        String clientId = getClientId(GlobalConfiguration.KEYCLOAK_CLIENT_ID.getValueAsString());
        if (userId != null && clientId != null) {

            // get the role id to assign
            String roleId = getClientRoleId(userId, clientId, roleName);
            if (roleId != null) {
                String url = String.format("%s/users/%s/role-mappings/clients/%s", getBaseKeycloakAdminUrl(), userId,
                        clientId);

                JSONObject jo = new JSONObject();
                jo.put("id", roleId);
                jo.put("name", roleName);
                JSONArray ja = new JSONArray();
                ja.put(jo);
                postAuthenticatedAndGetResponse(url, ja.toString());
            }
        }
    }
}
