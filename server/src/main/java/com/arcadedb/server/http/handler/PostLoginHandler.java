package com.arcadedb.server.http.handler;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.undertow.server.HttpServerExchange;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class PostLoginHandler extends AbstractHandler {
    public PostLoginHandler(final HttpServer httpServer) {
        super(httpServer);
    }

    @Override
    protected boolean mustExecuteOnWorkerThread() {
        return true;
    }

    @Override
    public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user) {
        final JSONObject payload = new JSONObject(parseRequestPayload(exchange));

        final String username = payload.has("username") ? payload.getString("username") : null;
        final String password = payload.has("password") ? payload.getString("password") : null;
        if (username == null || password == null) {
            // TODO return error
        }

        // TODO replace with keycloak config, or use keycloak login GUI
        Map<String, String> formData = new HashMap<>();
        formData.put("username", username);
        formData.put("password", password);
        formData.put("grant_type", "password");
        formData.put("scope", "openid");
        formData.put("client_id", GlobalConfiguration.KEYCLOAK_CLIENT_ID.getValueAsString());
        formData.put("client_secret", System.getenv("KEYCLOAK_CLIENT_SECRET"));

        String keycloakRootUrl = GlobalConfiguration.KEYCLOAK_ROOT_URL.getValueAsString();
        String uri = String.format("%s/auth/realms/data-fabric/protocol/openid-connect/token", keycloakRootUrl);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(uri))
                .POST(HttpRequest.BodyPublishers.ofString(getFormDataAsString(formData)))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .build();

        HttpClient client = HttpClient.newHttpClient();
        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                JsonObject responseJson = new JsonParser().parse(response.body()).getAsJsonObject();
                return new ExecutionResponse(200, response.body());
            }
        } catch (IOException e) {
            return new ExecutionResponse(500, e.getMessage());
        } catch (InterruptedException e) {
            return new ExecutionResponse(500, e.getMessage());
        }

        return new ExecutionResponse(500, "Error authenticating user");
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

    @Override
    public boolean isRequireAuthentication() {
        return false;
    }
}
