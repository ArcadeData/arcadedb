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
package com.arcadedb.server.http.handler;

import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.openapi.AiApiSpec;
import com.arcadedb.server.http.handler.openapi.AuthApiSpec;
import com.arcadedb.server.http.handler.openapi.CoreApiSpec;
import com.arcadedb.server.http.handler.openapi.GrafanaApiSpec;
import com.arcadedb.server.http.handler.openapi.McpApiSpec;
import com.arcadedb.server.http.handler.openapi.OpenApiContributor;
import com.arcadedb.server.http.handler.openapi.PluginApiSpec;
import com.arcadedb.server.http.handler.openapi.PrometheusApiSpec;
import com.arcadedb.server.http.handler.openapi.SecurityAdminApiSpec;
import com.arcadedb.server.http.handler.openapi.TimeSeriesApiSpec;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.servers.Server;
import io.swagger.v3.oas.models.tags.Tag;

import java.util.List;

/**
 * Generates the OpenAPI 3.0 specification for the ArcadeDB HTTP API. The document is assembled by
 * a fixed list of {@link OpenApiContributor}s, one per API domain: this class owns only the
 * document-level parts, so adding endpoints never grows it.
 * <p>
 * Security is declared once at the root as basicAuth or bearerAuth, because
 * {@code AbstractServerHttpHandler} accepts both on every authenticated route. Operations that
 * deviate override it themselves.
 */
public class OpenApiSpecGenerator {

  // A contributor absent from this list never reaches the served document: its paths and schemas
  // are silently omitted, with no compile error and no other signal. Order affects nothing but the
  // key order of the emitted paths object.
  private static final List<OpenApiContributor> CONTRIBUTORS = List.of(//
      new CoreApiSpec(), //
      new AuthApiSpec(), //
      new SecurityAdminApiSpec(), //
      new TimeSeriesApiSpec(), //
      new GrafanaApiSpec(), //
      new PrometheusApiSpec(), //
      new AiApiSpec(), //
      new McpApiSpec(), //
      new PluginApiSpec());

  /**
   * The contributors that assemble the served document, in registration order.
   */
  static List<OpenApiContributor> contributors() {
    return CONTRIBUTORS;
  }

  public OpenApiSpecGenerator(final HttpServer httpServer) {
  }

  public OpenAPI generateSpec() {
    final OpenAPI openAPI = new OpenAPI();
    openAPI.setOpenapi("3.0.3");
    openAPI.setInfo(createApiInfo());
    openAPI.setServers(createServers());
    openAPI.setTags(createTags());
    openAPI.setComponents(createComponents());
    openAPI.setPaths(new Paths());
    openAPI.setSecurity(List.of(//
        new SecurityRequirement().addList("basicAuth"), //
        new SecurityRequirement().addList("bearerAuth")));

    // Four registered routes are deliberately absent from this document.
    //
    // GET /api/v1/openapi.json serves this document. Describing its own retrieval gives a
    //   generated client no capability it does not already have by construction.
    // GET /api/v1/docs serves the Swagger UI page. It is HTML for a human, not an API operation.
    // /ws is a WebSocket upgrade. OpenAPI 3.0 cannot express a bidirectional stream under any
    //   encoding; AsyncAPI is the IDL that would, and adopting it is not in scope here.
    // / is the Studio static-content fallback, registered only outside production mode or when
    //   STUDIO_ENABLED is set. Assets, not an API.
    //
    // Adding a route to this list is a decision, not a shortcut: OpenApiSpecGenerationIT asserts
    // the exact operation inventory, so an accidental omission fails the build instead.
    for (final OpenApiContributor contributor : CONTRIBUTORS)
      contributor.contribute(openAPI);

    return openAPI;
  }

  private Info createApiInfo() {
    final Info info = new Info();
    info.setTitle("ArcadeDB HTTP API");
    info.setDescription(
        "Multi-Model DBMS HTTP API for Graph, Document, Key/Value, Search Engine, Time Series, and Vector Embedding operations");
    info.setVersion("1.0.0");

    final Contact contact = new Contact();
    contact.setName("Arcade Data Ltd");
    contact.setEmail("info@arcadedata.com");
    contact.setUrl("https://arcadedb.com");
    info.setContact(contact);

    final License license = new License();
    license.setName("Apache 2.0");
    license.setUrl("https://www.apache.org/licenses/LICENSE-2.0");
    info.setLicense(license);

    return info;
  }

  private List<Server> createServers() {
    final Server server = new Server();
    server.setUrl("http://localhost:2480");
    server.setDescription("ArcadeDB Server");
    return List.of(server);
  }

  /**
   * The tag vocabulary is a public contract: client generators derive an API class per tag, so a
   * renamed or invented tag renames a generated class. Keep this list closed.
   */
  private List<Tag> createTags() {
    return List.of(//
        tag("Server", "Server information and administrative commands"), //
        tag("Health", "Liveness and readiness probes"), //
        tag("Database", "Database existence and enumeration"), //
        tag("Query", "Read-only query execution"), //
        tag("Command", "Data and schema modification"), //
        tag("Transaction", "Explicit transaction lifecycle"), //
        tag("Batch", "Streaming bulk ingestion"), //
        tag("Auth", "Session login, logout, and enumeration"), //
        tag("Security", "Users, groups, and API tokens"), //
        tag("TimeSeries", "Time-series ingestion and querying"), //
        tag("Grafana", "Grafana data source endpoints"), //
        tag("Prometheus", "Prometheus remote read and write"), //
        tag("PromQL", "Prometheus-compatible query API"), //
        tag("AI", "AI assistant configuration and chat"), //
        tag("MCP", "Model Context Protocol endpoint and configuration"), //
        tag("Cluster", "Raft high-availability cluster management"), //
        tag("Metrics", "Metrics scrape endpoints"));
  }

  private Tag tag(final String name, final String description) {
    final Tag tag = new Tag();
    tag.setName(name);
    tag.setDescription(description);
    return tag;
  }

  private Components createComponents() {
    final Components components = new Components();

    final SecurityScheme basicAuth = new SecurityScheme();
    basicAuth.setType(SecurityScheme.Type.HTTP);
    basicAuth.setScheme("basic");
    basicAuth.setDescription("Basic HTTP authentication with username and password");
    components.addSecuritySchemes("basicAuth", basicAuth);

    // One scheme covers both token kinds: an API token ('at-' prefix, minted at
    // POST /api/v1/server/api-tokens) and a session token ('AU-' prefix, minted at
    // POST /api/v1/login). OpenAPI cannot distinguish two http/bearer schemes, so the
    // distinction lives in the description.
    //
    // X-ArcadeDB-Cluster-Token is deliberately not declared. It is cluster-internal
    // peer-to-peer authentication paired with X-ArcadeDB-Forwarded-User, not a scheme a
    // client should ever present.
    final SecurityScheme bearerAuth = new SecurityScheme();
    bearerAuth.setType(SecurityScheme.Type.HTTP);
    bearerAuth.setScheme("bearer");
    bearerAuth.setDescription("""
        Token authentication. Accepts an API token prefixed 'at-', created at \
        POST /api/v1/server/api-tokens, or a session token prefixed 'AU-', returned by \
        POST /api/v1/login.""");
    components.addSecuritySchemes("bearerAuth", bearerAuth);

    return components;
  }
}
