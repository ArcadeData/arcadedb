/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.http;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.handler.OpenApiSpecGenerator;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.responses.ApiResponses;
import io.swagger.v3.oas.models.tags.Tag;
import io.swagger.v3.parser.OpenAPIV3Parser;
import io.swagger.v3.parser.core.models.ParseOptions;
import io.swagger.v3.parser.core.models.SwaggerParseResult;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Integration test for OpenAPI specification generation and validation.
 * This test follows TDD principles by testing the expected behavior before implementation.
 *
 * The test verifies that:
 * 1. An OpenAPI spec endpoint is available at /api/v1/openapi.json
 * 2. The spec includes all discovered HTTP endpoints
 * 3. The spec is valid OpenAPI 3.0+ format
 * 4. The spec includes proper request/response models
 *
 * Expected endpoints to be documented:
 * - GET /api/v1/server (server information)
 * - GET /api/v1/ready (readiness check)
 * - GET /api/v1/databases (list databases)
 * - GET /api/v1/exists/{database} (check database existence)
 * - GET /api/v1/query/{database}/{language}/{command} (query via GET)
 * - POST /api/v1/query/{database} (query via POST)
 * - POST /api/v1/command/{database} (execute command)
 * - POST /api/v1/begin/{database} (begin transaction)
 * - POST /api/v1/commit/{database} (commit transaction)
 * - POST /api/v1/rollback/{database} (rollback transaction)
 * - POST /api/v1/server (server commands)
 */
class OpenApiSpecGenerationIT extends BaseGraphServerTest {
  private final HttpClient client = HttpClient.newHttpClient();

  @Test
  void openApiSpecEndpointIsAccessible() throws Exception {
    // Test that the OpenAPI spec endpoint exists and returns valid JSON
    HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/openapi.json"))
        .GET()
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

    assertThat(response.statusCode())
        .as("OpenAPI spec endpoint should be accessible")
        .isEqualTo(200);

    assertThat(response.headers().firstValue("Content-Type").orElse(""))
        .as("OpenAPI spec should return JSON content type")
        .contains("application/json");

    // Verify the response body is valid JSON
    JSONObject specJson = new JSONObject(response.body());
    assertThat(specJson)
        .as("OpenAPI spec should be valid JSON")
        .isNotNull();
  }

  @Test
  void openApiSpecIsValidOpenApi30Format() throws Exception {
    // Retrieve the OpenAPI spec
    String specContent = getOpenApiSpec();

    // Parse and validate the OpenAPI spec using the official parser
    OpenAPIV3Parser parser = new OpenAPIV3Parser();
    ParseOptions options = new ParseOptions();
    options.setResolve(true);
    options.setValidateExternalRefs(false);

    SwaggerParseResult result = parser.readContents(specContent, null, options);

    assertThat(result.getMessages())
        .as("OpenAPI spec should not have parsing errors: %s", result.getMessages())
        .isEmpty();

    OpenAPI openAPI = result.getOpenAPI();
    assertThat(openAPI)
        .as("OpenAPI spec should be parseable")
        .isNotNull();

    // Verify OpenAPI version
    assertThat(openAPI.getOpenapi())
        .as("OpenAPI spec should be version 3.0+")
        .startsWith("3.");

    // Verify basic info is present
    Info info = openAPI.getInfo();
    assertThat(info)
        .as("OpenAPI spec should have info section")
        .isNotNull();

    assertThat(info.getTitle())
        .as("OpenAPI spec should have a title")
        .isNotBlank();

    assertThat(info.getVersion())
        .as("OpenAPI spec should have a version")
        .isNotBlank();
  }

  @Test
  void openApiSpecIncludesAllExpectedEndpoints() throws Exception {
    // Retrieve and parse the OpenAPI spec
    String specContent = getOpenApiSpec();
    OpenAPI openAPI = new OpenAPIV3Parser().readContents(specContent).getOpenAPI();

    assertThat(openAPI)
        .as("OpenAPI spec should be parseable")
        .isNotNull();

    Map<String, PathItem> paths = openAPI.getPaths();
    assertThat(paths)
        .as("OpenAPI spec should have paths defined")
        .isNotNull()
        .isNotEmpty();

    // Define expected endpoints with their HTTP methods
    Set<String> expectedGetEndpoints = Set.of(
        "/api/v1/server",
        "/api/v1/ready",
        "/api/v1/health",
        "/api/v1/databases",
        "/api/v1/exists/{database}",
        "/api/v1/query/{database}/{language}/{command}"
    );

    Set<String> expectedPostEndpoints = Set.of(
        "/api/v1/query/{database}",
        "/api/v1/command/{database}",
        "/api/v1/begin/{database}",
        "/api/v1/commit/{database}",
        "/api/v1/rollback/{database}",
        "/api/v1/server"
    );

    // Verify all expected GET endpoints are documented
    for (String expectedPath : expectedGetEndpoints) {
      assertThat(paths)
          .as("OpenAPI spec should include GET endpoint: %s", expectedPath)
          .containsKey(expectedPath);

      PathItem pathItem = paths.get(expectedPath);
      assertThat(pathItem.getGet())
          .as("GET operation should be defined for path: %s", expectedPath)
          .isNotNull();
    }

    // Verify all expected POST endpoints are documented
    for (String expectedPath : expectedPostEndpoints) {
      assertThat(paths)
          .as("OpenAPI spec should include POST endpoint: %s", expectedPath)
          .containsKey(expectedPath);

      PathItem pathItem = paths.get(expectedPath);
      assertThat(pathItem.getPost())
          .as("POST operation should be defined for path: %s", expectedPath)
          .isNotNull();
    }
  }

  @Test
  void openApiSpecIncludesProperRequestResponseModels() throws Exception {
    // Retrieve and parse the OpenAPI spec
    String specContent = getOpenApiSpec();
    OpenAPI openAPI = new OpenAPIV3Parser().readContents(specContent).getOpenAPI();

    assertThat(openAPI)
        .as("OpenAPI spec should be parseable")
        .isNotNull();

    Map<String, PathItem> paths = openAPI.getPaths();

    // Verify that operations have proper responses defined
    for (Map.Entry<String, PathItem> pathEntry : paths.entrySet()) {
      String path = pathEntry.getKey();
      PathItem pathItem = pathEntry.getValue();

      // Check GET operations
      if (pathItem.getGet() != null) {
        Operation getOp = pathItem.getGet();
        assertThat(getOp.getResponses())
            .as("GET operation for %s should have responses defined", path)
            .isNotNull()
            .isNotEmpty();

        // Should at least have a success response (200 or 204)
        ApiResponses responses = getOp.getResponses();
        boolean hasSuccessResponse = responses.get("200") != null || responses.get("204") != null;
        assertThat(hasSuccessResponse)
            .as("GET operation for %s should have success response (200 or 204)", path)
            .isTrue();
      }

      // Check POST operations
      if (pathItem.getPost() != null) {
        Operation postOp = pathItem.getPost();
        assertThat(postOp.getResponses())
            .as("POST operation for %s should have responses defined", path)
            .isNotNull()
            .isNotEmpty();

        // Should at least have a success response (200, 201, or 204)
        ApiResponses responses = postOp.getResponses();
        boolean hasSuccessResponse = responses.get("200") != null || responses.get("201") != null
            || responses.get("204") != null;
        assertThat(hasSuccessResponse)
            .as("POST operation for %s should have success response (200, 201, or 204)", path)
            .isTrue();
      }
    }
  }

  @Test
  void openApiSpecIncludesSecurityInformation() throws Exception {
    // Retrieve and parse the OpenAPI spec
    String specContent = getOpenApiSpec();
    OpenAPI openAPI = new OpenAPIV3Parser().readContents(specContent).getOpenAPI();

    assertThat(openAPI)
        .as("OpenAPI spec should be parseable")
        .isNotNull();

    // Verify that security schemes are defined (since ArcadeDB uses Basic Auth)
    assertThat(openAPI.getComponents())
        .as("OpenAPI spec should have components section")
        .isNotNull();

    if (openAPI.getComponents().getSecuritySchemes() != null) {
      assertThat(openAPI.getComponents().getSecuritySchemes())
          .as("OpenAPI spec should define security schemes")
          .isNotEmpty();
    }
  }

  @Test
  void openApiSpecGeneratorClassExists() {
    // This test verifies that the OpenApiSpecGenerator class can be instantiated
    // The class requires an HttpServer instance which we get from the test server
    try {
      Class<?> generatorClass = Class.forName("com.arcadedb.server.http.handler.OpenApiSpecGenerator");
      assertThat(generatorClass)
          .as("OpenApiSpecGenerator class should exist")
          .isNotNull();

      // Get the HttpServer from the test server instance
      HttpServer httpServer = getServer(0).getHttpServer();
      assertThat(httpServer)
          .as("HttpServer should be available from test server")
          .isNotNull();

      // Instantiate the generator with the HttpServer dependency
      OpenApiSpecGenerator generator = new OpenApiSpecGenerator(httpServer);
      assertThat(generator)
          .as("OpenApiSpecGenerator should be instantiable with HttpServer")
          .isNotNull();

      // Verify it can generate a spec
      OpenAPI spec = generator.generateSpec();
      assertThat(spec)
          .as("OpenApiSpecGenerator should generate OpenAPI spec")
          .isNotNull();

    } catch (ClassNotFoundException e) {
      fail("OpenApiSpecGenerator class not found. Expected at: com.arcadedb.server.http.handler.OpenApiSpecGenerator");
    } catch (Exception e) {
      fail("Failed to instantiate OpenApiSpecGenerator: " + e.getMessage());
    }
  }

  @Test
  void rootSecurityDeclaresBasicAndBearer() throws Exception {
    final OpenAPI openAPI = new OpenAPIV3Parser().readContents(getOpenApiSpec()).getOpenAPI();

    assertThat(openAPI.getSecurity())
        .as("root security should offer basicAuth and bearerAuth as alternatives")
        .hasSize(2);
    assertThat(openAPI.getSecurity().stream().flatMap(r -> r.keySet().stream()).toList())
        .containsExactlyInAnyOrder("basicAuth", "bearerAuth");
  }

  @Test
  void livenessAndReadinessAreDeclaredPublic() throws Exception {
    final OpenAPI openAPI = new OpenAPIV3Parser().readContents(getOpenApiSpec()).getOpenAPI();

    for (final String path : List.of("/api/v1/health", "/api/v1/ready")) {
      assertThat(openAPI.getPaths().get(path).getGet().getSecurity())
          .as("%s requires no authentication, so it must override root security with an empty list", path)
          .isNotNull()
          .isEmpty();
    }
  }

  @Test
  void apiTokenAuthenticatesAgainstTheDeclaredBearerScheme() throws Exception {
    // Mint a real API token through the documented endpoint.
    final HttpRequest createToken = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/server/api-tokens"))
        .header("Content-Type", "application/json")
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .POST(HttpRequest.BodyPublishers.ofString("{\"name\":\"openapi-bearer-test\"}"))
        .build();

    final HttpResponse<String> created = client.send(createToken, BodyHandlers.ofString());
    assertThat(created.statusCode())
        .as("token creation should succeed: %s", created.body())
        .isIn(200, 201);

    // PostApiTokenHandler wraps the token payload under a "result" object rather than
    // returning it at the top level.
    final String token = new JSONObject(created.body()).getJSONObject("result").getString("token");
    assertThat(token).startsWith("at-");

    // The server accepts it as a bearer token on an ordinary documented operation.
    final HttpRequest listDatabases = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/databases"))
        .GET()
        .setHeader("Authorization", "Bearer " + token)
        .build();

    final HttpResponse<String> listed = client.send(listDatabases, BodyHandlers.ofString());
    assertThat(listed.statusCode())
        .as("an 'at-' API token must authenticate as a bearer token: %s", listed.body())
        .isEqualTo(200);

    // And the spec says so: root security offers bearerAuth, and this operation does not override it.
    final OpenAPI openAPI = new OpenAPIV3Parser().readContents(getOpenApiSpec()).getOpenAPI();
    assertThat(openAPI.getComponents().getSecuritySchemes().get("bearerAuth").getScheme())
        .isEqualTo("bearer");
    assertThat(openAPI.getPaths().get("/api/v1/databases").getGet().getSecurity())
        .as("listDatabases must inherit root security rather than override it")
        .isNull();
    assertThat(openAPI.getSecurity().stream().flatMap(r -> r.keySet().stream()).toList())
        .contains("bearerAuth");
  }

  /**
   * Every operation the specification must document, as "METHOD path". Deliberately exhaustive and
   * deliberately hand-written: a list derived from the specification under test would assert nothing.
   */
  private static final List<String> EXPECTED_OPERATIONS = List.of(
      // Core
      "GET /api/v1/server", "POST /api/v1/server",
      "GET /api/v1/ready", "GET /api/v1/health",
      "GET /api/v1/databases", "GET /api/v1/exists/{database}",
      "GET /api/v1/query/{database}/{language}/{command}", "POST /api/v1/query/{database}",
      "POST /api/v1/command/{database}", "POST /api/v1/batch/{database}",
      "GET /api/v1/progress/{database}",
      "POST /api/v1/begin/{database}", "POST /api/v1/commit/{database}",
      "POST /api/v1/rollback/{database}",
      // Auth
      "POST /api/v1/login", "POST /api/v1/logout", "GET /api/v1/sessions",
      // Security admin
      "GET /api/v1/server/users", "POST /api/v1/server/users",
      "PUT /api/v1/server/users", "DELETE /api/v1/server/users",
      "GET /api/v1/server/groups", "POST /api/v1/server/groups", "DELETE /api/v1/server/groups",
      "GET /api/v1/server/api-tokens", "POST /api/v1/server/api-tokens",
      "DELETE /api/v1/server/api-tokens",
      // Time-series
      "POST /api/v1/ts/{database}/write", "POST /api/v1/ts/{database}/query",
      "GET /api/v1/ts/{database}/latest",
      // Grafana
      "GET /api/v1/ts/{database}/grafana/health", "GET /api/v1/ts/{database}/grafana/metadata",
      "POST /api/v1/ts/{database}/grafana/query",
      // Prometheus and PromQL
      "POST /api/v1/ts/{database}/prom/write", "POST /api/v1/ts/{database}/prom/read",
      "GET /api/v1/ts/{database}/prom/api/v1/query",
      "GET /api/v1/ts/{database}/prom/api/v1/query_range",
      "GET /api/v1/ts/{database}/prom/api/v1/labels",
      "GET /api/v1/ts/{database}/prom/api/v1/label/{name}/values",
      "GET /api/v1/ts/{database}/prom/api/v1/series",
      // MCP
      "POST /api/v1/mcp", "GET /api/v1/mcp/config", "POST /api/v1/mcp/config",
      // AI
      "GET /api/v1/ai/config", "POST /api/v1/ai/activate", "POST /api/v1/ai/chat",
      "POST /api/v1/ai/analyze-profiler", "GET /api/v1/ai/chats",
      "GET /api/v1/ai/chats/{id}", "PUT /api/v1/ai/chats/{id}", "DELETE /api/v1/ai/chats/{id}",
      // Plugin-contributed
      "GET /prometheus",
      "GET /api/v1/cluster", "POST /api/v1/cluster/peer", "DELETE /api/v1/cluster/peer/{peerId}",
      "POST /api/v1/cluster/leader", "POST /api/v1/cluster/stepdown", "POST /api/v1/cluster/leave",
      "POST /api/v1/cluster/verify/{database}", "POST /api/v1/cluster/resync/{database}",
      "POST /api/v1/cluster/bootstrap-state",
      "GET /api/v1/ha/snapshot/{database}", "GET /api/v1/ha/snapshot/{database}/checksums");

  private static List<String> declaredOperations(final OpenAPI openAPI) {
    final List<String> declared = new ArrayList<>();
    openAPI.getPaths().forEach((path, item) -> {
      if (item.getGet() != null)
        declared.add("GET " + path);
      if (item.getPost() != null)
        declared.add("POST " + path);
      if (item.getPut() != null)
        declared.add("PUT " + path);
      if (item.getDelete() != null)
        declared.add("DELETE " + path);
      if (item.getPatch() != null)
        declared.add("PATCH " + path);
      if (item.getHead() != null)
        declared.add("HEAD " + path);
      if (item.getOptions() != null)
        declared.add("OPTIONS " + path);
    });
    return declared;
  }

  @Test
  void specDocumentsExactlyTheExpectedSixtyThreeOperations() throws Exception {
    final OpenAPI openAPI = new OpenAPIV3Parser().readContents(getOpenApiSpec()).getOpenAPI();
    final List<String> declared = declaredOperations(openAPI);

    assertThat(EXPECTED_OPERATIONS)
        .as("the inventory itself must hold no duplicate")
        .doesNotHaveDuplicates()
        .hasSize(63);

    assertThat(declared)
        .as("operations missing from the specification")
        .containsAll(EXPECTED_OPERATIONS);

    assertThat(declared)
        .as("operations in the specification with no registered handler: reverse drift")
        .containsExactlyInAnyOrderElementsOf(EXPECTED_OPERATIONS);
  }

  @Test
  void specDoesNotDocumentItselfOrTheWebSocketUpgrade() throws Exception {
    final OpenAPI openAPI = new OpenAPIV3Parser().readContents(getOpenApiSpec()).getOpenAPI();
    assertThat(openAPI.getPaths().keySet())
        .as("self-documentation, the Swagger UI page, the WebSocket upgrade, and the Studio static-content "
            + "fallback all stay out")
        .doesNotContain("/api/v1/openapi.json", "/api/v1/docs", "/ws", "/");
  }

  @Test
  void everyOperationIdIsUniqueAcrossTheWholeSpec() throws Exception {
    final OpenAPI openAPI = new OpenAPIV3Parser().readContents(getOpenApiSpec()).getOpenAPI();

    final List<String> ids = new ArrayList<>();
    openAPI.getPaths().values().forEach(item ->
        item.readOperations().forEach(op -> ids.add(op.getOperationId())));

    assertThat(ids)
        .as("client generators derive a method name per operationId, so a collision breaks codegen")
        .doesNotHaveDuplicates()
        .doesNotContainNull()
        .hasSize(63);
  }

  @Test
  void everyOperationCarriesExactlyOneDeclaredTag() throws Exception {
    final OpenAPI openAPI = new OpenAPIV3Parser().readContents(getOpenApiSpec()).getOpenAPI();
    final Set<String> declaredTags = openAPI.getTags().stream()
        .map(Tag::getName).collect(Collectors.toSet());

    openAPI.getPaths().forEach((path, item) -> item.readOperations().forEach(op -> {
      assertThat(op.getTags())
          .as("%s %s: generators derive an API class from the first tag", path, op.getOperationId())
          .hasSize(1);
      assertThat(declaredTags)
          .as("%s uses tag %s, which is not in the root tag vocabulary", op.getOperationId(),
              op.getTags().getFirst())
          .contains(op.getTags().getFirst());
    }));
  }

  @Test
  void everyReferenceResolvesAndTheDocumentValidatesClean() throws Exception {
    final ParseOptions options = new ParseOptions();
    options.setResolve(true);
    options.setResolveFully(true);
    options.setValidateExternalRefs(false);

    final SwaggerParseResult result = new OpenAPIV3Parser().readContents(getOpenApiSpec(), null, options);

    assertThat(result.getMessages())
        .as("an unresolved $ref or a malformed schema shows up here: %s", result.getMessages())
        .isEmpty();
  }

  @Test
  void everyOperationDeclaresASuccessAndAnAuthFailureResponse() throws Exception {
    final OpenAPI openAPI = new OpenAPIV3Parser().readContents(getOpenApiSpec()).getOpenAPI();

    openAPI.getPaths().forEach((path, item) -> item.readOperations().forEach(op -> {
      assertThat(op.getResponses())
          .as("%s %s has no responses", path, op.getOperationId())
          .isNotNull().isNotEmpty();

      final boolean hasSuccess = op.getResponses().keySet().stream()
          .anyMatch(code -> code.startsWith("2"));
      assertThat(hasSuccess)
          .as("%s %s declares no 2xx response", path, op.getOperationId())
          .isTrue();

      // Health and readiness are declared public, so 401 would be a lie for them.
      final boolean isPublic = op.getSecurity() != null && op.getSecurity().isEmpty();
      if (!isPublic) {
        assertThat(op.getResponses().keySet())
            .as("%s %s is authenticated, so it must declare 401", path, op.getOperationId())
            .contains("401");
      }
    }));
  }

  /**
   * Helper method to retrieve the OpenAPI specification from the server.
   *
   * @return The OpenAPI specification as a JSON string
   * @throws Exception if the request fails
   */
  private String getOpenApiSpec() throws Exception {
    HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/openapi.json"))
        .GET()
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      fail("Failed to retrieve OpenAPI spec. Status: " + response.statusCode() + ", Body: " + response.body());
    }

    return response.body();
  }
}
