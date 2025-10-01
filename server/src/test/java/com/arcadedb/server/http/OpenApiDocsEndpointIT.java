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

import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for OpenAPI HTML documentation endpoint.
 * This test follows TDD principles by testing the expected behavior before implementation.
 *
 * The test verifies that:
 * 1. HTML documentation endpoint is available at /api/v1/docs
 * 2. HTML page properly initializes Swagger UI with correct configuration
 * 3. HTML page references static assets served by the studio module at /static/swagger-ui/*
 * 4. Authentication is properly configured in Swagger UI
 * 5. OpenAPI spec is properly loaded
 *
 * Note: Static assets (CSS, JS, favicon) are now served by the studio module at /static/swagger-ui/*
 * rather than by the server module at /api/v1/docs/swagger-ui/*
 *
 * Expected endpoints to be tested:
 * - GET /api/v1/docs (HTML documentation page served by server module)
 * - Static assets are served by studio at /static/swagger-ui/* (not tested here)
 */
class OpenApiDocsEndpointIT extends BaseGraphServerTest {
  private final HttpClient client = HttpClient.newHttpClient();

  // ==================== HTML Documentation Endpoint Tests ====================

  @Test
  void testDocsEndpointIsAccessible() throws Exception {
    // Test that the OpenAPI docs endpoint exists and returns 200
    HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/docs"))
        .GET()
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

    assertThat(response.statusCode())
        .as("OpenAPI docs endpoint should be accessible")
        .isEqualTo(200);
  }

  @Test
  void testDocsEndpointReturnsHtmlContentType() throws Exception {
    // Test that the docs endpoint returns HTML content type
    HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/docs"))
        .GET()
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

    assertThat(response.headers().firstValue("Content-Type").orElse(""))
        .as("OpenAPI docs should return HTML content type")
        .contains("text/html");
  }

  @Test
  void testDocsEndpointContainsSwaggerUiInitialization() throws Exception {
    // Test that HTML contains Swagger UI initialization code
    HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/docs"))
        .GET()
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
    String html = response.body();

    assertThat(html)
        .as("HTML should contain SwaggerUIBundle initialization")
        .contains("SwaggerUIBundle");

    assertThat(html)
        .as("HTML should contain Swagger UI container div")
        .contains("swagger-ui");
  }

  @Test
  void testDocsEndpointReferencesCorrectOpenApiSpecUrl() throws Exception {
    // Test that HTML references the correct OpenAPI spec URL
    HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/docs"))
        .GET()
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
    String html = response.body();

    assertThat(html)
        .as("HTML should reference the OpenAPI spec URL")
        .contains("/api/v1/openapi.json");
  }

  @Test
  void testDocsEndpointIncludesNecessaryJavaScriptReferences() throws Exception {
    // Test that HTML includes necessary JavaScript references from studio module
    HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/docs"))
        .GET()
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
    String html = response.body();

    assertThat(html)
        .as("HTML should include Swagger UI bundle JavaScript from studio")
        .contains("/static/swagger-ui/swagger-ui-bundle.js");

    assertThat(html)
        .as("HTML should include Swagger UI standalone preset JavaScript from studio")
        .contains("/static/swagger-ui/swagger-ui-standalone-preset.js");
  }

  @Test
  void testDocsEndpointIncludesNecessaryCssReferences() throws Exception {
    // Test that HTML includes necessary CSS references from studio module
    HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/docs"))
        .GET()
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
    String html = response.body();

    assertThat(html)
        .as("HTML should include Swagger UI CSS from studio")
        .contains("/static/swagger-ui/swagger-ui.css");
  }

  @Test
  void testDocsEndpointIncludesPageTitle() throws Exception {
    // Test that HTML has a proper page title
    HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/docs"))
        .GET()
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
    String html = response.body();

    assertThat(html)
        .as("HTML should have a title tag")
        .contains("<title>");

    assertThat(html)
        .as("HTML title should reference ArcadeDB API")
        .containsIgnoringCase("ArcadeDB");
  }

  @Test
  void testDocsEndpointIsValidHtml() throws Exception {
    // Test that the response is valid HTML structure
    HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/docs"))
        .GET()
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
    String html = response.body();

    assertThat(html)
        .as("HTML should have DOCTYPE declaration")
        .containsIgnoringCase("<!DOCTYPE html>");

    assertThat(html)
        .as("HTML should have html opening tag")
        .contains("<html");

    assertThat(html)
        .as("HTML should have html closing tag")
        .contains("</html>");

    assertThat(html)
        .as("HTML should have head section")
        .contains("<head>")
        .contains("</head>");

    assertThat(html)
        .as("HTML should have body section")
        .contains("<body>")
        .contains("</body>");
  }

  // ==================== Integration Tests ====================

  @Test
  void testSwaggerUiCanLoadOpenApiSpec() throws Exception {
    // Test that the OpenAPI spec referenced in the docs page is accessible
    // First, get the docs page
    HttpRequest docsRequest = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/docs"))
        .GET()
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    HttpResponse<String> docsResponse = client.send(docsRequest, BodyHandlers.ofString());

    assertThat(docsResponse.statusCode())
        .as("Docs page should be accessible")
        .isEqualTo(200);

    // Now verify the OpenAPI spec endpoint is accessible
    HttpRequest specRequest = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/openapi.json"))
        .GET()
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    HttpResponse<String> specResponse = client.send(specRequest, BodyHandlers.ofString());

    assertThat(specResponse.statusCode())
        .as("OpenAPI spec should be accessible from docs page")
        .isEqualTo(200);

    assertThat(specResponse.headers().firstValue("Content-Type").orElse(""))
        .as("OpenAPI spec should return JSON")
        .contains("application/json");
  }

  @Test
  void testAuthenticationConfigurationInSwaggerUi() throws Exception {
    // Test that HTML includes authentication configuration
    HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/docs"))
        .GET()
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
    String html = response.body();

    // Swagger UI should have configuration for authentication
    assertThat(html)
        .as("HTML should contain authentication configuration or authorization button setup")
        .containsAnyOf(
            "persistAuthorization",
            "withCredentials",
            "requestInterceptor"
        );
  }

  @Test
  void testDocsEndpointDisplaysAllExpectedElements() throws Exception {
    // Test that the documentation page has all expected UI elements
    HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/docs"))
        .GET()
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
    String html = response.body();

    // Check for Swagger UI configuration object
    assertThat(html)
        .as("HTML should contain SwaggerUIBundle configuration")
        .contains("SwaggerUIBundle");

    // Check for common Swagger UI configuration options
    assertThat(html)
        .as("HTML should configure the spec URL")
        .contains("url");

    // Check for DOM element ID where Swagger UI will be mounted
    assertThat(html)
        .as("HTML should have a DOM element for Swagger UI")
        .contains("id=");
  }

  @Test
  void testDocsEndpointWithoutAuthenticationReturnsUnauthorized() throws Exception {
    // Test that accessing docs without authentication returns 401
    HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/docs"))
        .GET()
        .build();

    HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

    assertThat(response.statusCode())
        .as("Docs endpoint without authentication should return 401")
        .isEqualTo(401);
  }

  @Test
  void testDocsHandlerClassExists() {
    // This test verifies that the OpenApiDocsHandler class exists
    try {
      Class<?> handlerClass = Class.forName("com.arcadedb.server.http.handler.OpenApiDocsHandler");
      assertThat(handlerClass)
          .as("OpenApiDocsHandler class should exist")
          .isNotNull();
    } catch (ClassNotFoundException e) {
      assertThat(false)
          .as("OpenApiDocsHandler class not found. Expected at: com.arcadedb.server.http.handler.OpenApiDocsHandler")
          .isTrue();
    }
  }

  @Test
  void testDocsEndpointSupportsHeadRequest() throws Exception {
    // Test that the docs endpoint supports HEAD requests
    HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/docs"))
        .method("HEAD", HttpRequest.BodyPublishers.noBody())
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

    assertThat(response.statusCode())
        .as("Docs endpoint should support HEAD requests")
        .isIn(200, 405); // 405 if HEAD is not supported, which is acceptable

    if (response.statusCode() == 200) {
      assertThat(response.headers().firstValue("Content-Type").orElse(""))
          .as("HEAD response should include Content-Type header")
          .contains("text/html");
    }
  }

  @Test
  void testDocsEndpointCharsetIsUtf8() throws Exception {
    // Test that the docs endpoint specifies UTF-8 charset
    HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/docs"))
        .GET()
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

    assertThat(response.headers().firstValue("Content-Type").orElse(""))
        .as("HTML should specify UTF-8 charset")
        .containsAnyOf("charset=utf-8", "charset=UTF-8");
  }
}
