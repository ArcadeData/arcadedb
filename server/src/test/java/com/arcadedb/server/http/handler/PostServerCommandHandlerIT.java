package com.arcadedb.server.http.handler;

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Base64;
import java.util.HashMap;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

class PostServerCommandHandlerIT extends BaseGraphServerTest {

  @Test
  void testSetServerSettingCommnad() throws Exception {

    HttpClient client = HttpClient.newHttpClient();
    HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/server"))
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject()
            .put("command", "set database setting graph `arcadedb.dateTimeFormat` \"yyyy-MM-dd HH:mm:ss.SSS\" ")
            .toString()))
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();


    HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(200);

    request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/server"))
        .GET()
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();


     response = client.send(request, BodyHandlers.ofString());

    assertThat(response.statusCode()).isEqualTo(200);
//    assertThat(JsonPa response.body()).contains("arcadedb.dateTimeFormat");
    System.out.println("response = " + response.body());

  }
}
