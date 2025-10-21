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
import io.swagger.v3.parser.OpenAPIV3Parser;
import io.swagger.v3.parser.core.models.ParseOptions;
import io.swagger.v3.parser.core.models.SwaggerParseResult;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Base64;
import java.util.Map;
import java.util.Set;

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
  void testOpenApiSpecEndpointIsAccessible() throws Exception {
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
  void testOpenApiSpecIsValidOpenApi30Format() throws Exception {
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
  void testOpenApiSpecIncludesAllExpectedEndpoints() throws Exception {
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
  void testOpenApiSpecIncludesProperRequestResponseModels() throws Exception {
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

        // Should at least have a 200 response
        ApiResponses responses = getOp.getResponses();
        assertThat(responses.get("200"))
            .as("GET operation for %s should have 200 response defined", path)
            .isNotNull();
      }

      // Check POST operations
      if (pathItem.getPost() != null) {
        Operation postOp = pathItem.getPost();
        assertThat(postOp.getResponses())
            .as("POST operation for %s should have responses defined", path)
            .isNotNull()
            .isNotEmpty();

        // Should at least have a success response (200 or 201)
        ApiResponses responses = postOp.getResponses();
        boolean hasSuccessResponse = responses.get("200") != null || responses.get("201") != null;
        assertThat(hasSuccessResponse)
            .as("POST operation for %s should have success response (200 or 201)", path)
            .isTrue();
      }
    }
  }

  @Test
  void testOpenApiSpecIncludesSecurityInformation() throws Exception {
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
  void testOpenApiSpecGeneratorClassExists() {
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
  void testOpenApiHandlerClassExists() {
    // This test verifies that the OpenApiHandler class can be instantiated
    // This will fail until we implement the class
    try {
      Class<?> handlerClass = Class.forName("com.arcadedb.server.http.handler.OpenApiHandler");
      assertThat(handlerClass)
          .as("OpenApiHandler class should exist")
          .isNotNull();
    } catch (ClassNotFoundException e) {
      fail("OpenApiHandler class not found. Expected at: com.arcadedb.server.http.handler.OpenApiHandler");
    }
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
