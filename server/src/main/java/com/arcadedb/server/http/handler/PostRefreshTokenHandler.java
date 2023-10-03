package com.arcadedb.server.http.handler;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;

public class PostRefreshTokenHandler extends AbstractHandler {
    public PostRefreshTokenHandler(final HttpServer httpServer) {
        super(httpServer);
    }

    @Override
    protected ExecutionResponse execute(HttpServerExchange exchange, ServerSecurityUser user) throws Exception {
        final JSONObject payload = new JSONObject(parseRequestPayload(exchange));
        final String refreshToken = payload.has("refreshToken") ? payload.getString("refreshToken") : null;
        if (refreshToken == null) {
            // TODO return error
        }

        // TODO replace with keycloak configuration
        Map<String, String> formData = new HashMap<>();
        formData.put("refresh_token", refreshToken);
        formData.put("grant_type", "refresh_token");
        formData.put("scope", "openid");
        formData.put("client_id", "df-backend");
        formData.put("client_secret", System.getenv("KEYCLOAK_CLIENT_SECRET"));

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://df-keycloak.auth.svc.cluster.local:8080/auth/realms/data-fabric/protocol/openid-connect/token"))
                .POST(HttpRequest.BodyPublishers.ofString(PostLoginHandler.getFormDataAsString(formData)))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .build();

        HttpClient client = HttpClient.newHttpClient();
        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                return new ExecutionResponse(200, response.body());
            }
        } catch (IOException e) {
            e.printStackTrace();
            return new ExecutionResponse(500, e.getMessage());
        } catch (InterruptedException e) {
            e.printStackTrace();
            return new ExecutionResponse(500, e.getMessage());
        }

        return new ExecutionResponse(500, "Error authenticating user");
    }

    @Override
    protected boolean mustExecuteOnWorkerThread() {
        return true;
    }

    @Override
    public boolean isRequireAuthentication() {
        return false;
    }
}
