package com.arcadedb.server;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataFabricRestClient {
    /**
     * Gets the non admin base url for keycloak. Suitable for login operations
     * 
     * @return
     */
    protected static String getBaseKeycloakUrl() {
        return String.format("%s/auth/realms/data-fabric", GlobalConfiguration.KEYCLOAK_ROOT_URL.getValueAsString());
    }

    /**
     * Gets the admin base url for keycloak. Suitable for admin operations like
     * getting user roles, creating roles, etc.
     * 
     * @return
     */
    protected static String getBaseKeycloakAdminUrl() {
        return String.format("%s/auth/admin/realms/data-fabric",
                GlobalConfiguration.KEYCLOAK_ROOT_URL.getValueAsString());
    }

    protected static String getLoginUrl() {
        return getBaseKeycloakUrl() + "/protocol/openid-connect/token";
    }

    protected static String login(String username, String password) {
        // TODO replace with keycloak config, or use keycloak login GUI
        Map<String, String> formData = new HashMap<>();
        formData.put("username", username);
        formData.put("password", password);
        formData.put("grant_type", "password");
        formData.put("scope", "openid");
        formData.put("client_id", GlobalConfiguration.KEYCLOAK_CLIENT_ID.getValueAsString());
        formData.put("client_secret", System.getenv("KEYCLOAK_CLIENT_SECRET"));
        return postUnauthenticatedAndGetResponse(getLoginUrl(), formData);
    }

    protected static String loginAndGetEncodedAccessString() {
        var login = login("admin", System.getenv("KEYCLOAK_ADMIN_PASSWORD"));

        JSONObject tokenJO = new JSONObject(login);
        return tokenJO.getString("access_token");
    }

    public static String getAccessTokenJsonFromResponse(String token) {
        if (token != null) {
            JSONObject tokenJO = new JSONObject(token);
            String accessTokenString = tokenJO.getString("access_token");
            String encodedString = accessTokenString.substring(accessTokenString.indexOf(".") + 1,
                    accessTokenString.lastIndexOf("."));
            byte[] decodedBytes = Base64.getDecoder().decode(encodedString);
            String decodedString = new String(decodedBytes);
            log.debug("getAccessTokenFromResponse {}", decodedString);

            return decodedString;
        }

        return null;
    }

    protected static String postUnauthenticatedAndGetResponse(String url, Map<String, String> formData) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .POST(HttpRequest.BodyPublishers.ofString(getFormDataAsString(formData)))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .build();

        return sendAndGetResponse(request);
    }

    public static String postAuthenticatedAndGetResponse(String url, String jsonPayload) {
        String accessTokenString = loginAndGetEncodedAccessString();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + accessTokenString)
                .timeout(Duration.ofSeconds(20))
                .build();

        return sendAndGetResponse(request);
    }

    protected static String putAuthenticatedAndGetResponse(String url, String jsonPayload) {
        String accessTokenString = loginAndGetEncodedAccessString();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .PUT(HttpRequest.BodyPublishers.ofString(jsonPayload))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + accessTokenString)
                .build();

        return sendAndGetResponse(request);
    }

    protected static String sendAndGetResponse(HttpRequest request) {
        HttpClient client = HttpClient.newHttpClient();
        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                return response.body();
            }
        } catch (IOException | InterruptedException e) {
            log.error("sendAndGetResponse()", e.getMessage());
            log.debug("Exception", e);
            return null;
        }

        return null;
    }

    protected static String sendAuthenticatedGetAndGetResponse(String url) {
        String accessTokenString = loginAndGetEncodedAccessString();

        // get user info
        // "http://localhost/auth/admin/realms/data-fabric/users";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .header("Authorization", "Bearer " + accessTokenString)
                .build();

        HttpClient client = HttpClient.newHttpClient();
        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                return response.body();
            }
        } catch (IOException | InterruptedException e) {
            log.error("sendAuthenticatedGetAndGetResponse()", e.getMessage());
            log.debug("Exception", e);
            return null;
        }

        return null;
    }

    public static String getFormDataAsString(Map<String, String> formData) {
        StringBuilder formBodyBuilder = new StringBuilder();
        for (Map.Entry<String, String> singleEntry : formData.entrySet()) {
            if (formBodyBuilder.length() > 0) {
                formBodyBuilder.append("&");
            }
            formBodyBuilder.append(URLEncoder.encode(singleEntry.getKey(), StandardCharsets.UTF_8));
            formBodyBuilder.append("=");
            formBodyBuilder.append(URLEncoder.encode(singleEntry.getValue(), StandardCharsets.UTF_8));
        }
        return formBodyBuilder.toString();
    }
}
