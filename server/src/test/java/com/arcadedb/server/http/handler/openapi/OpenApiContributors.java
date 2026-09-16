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
package com.arcadedb.server.http.handler.openapi;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.media.Schema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds the document one contributor at a time, and walks it.
 * <p>
 * Shared by the sweeps of issues #7577 and #7578, which both have to reach every schema a contributor registers
 * <em>and</em> every schema declared inline under a path - a request body or a response that names no component
 * is just as reachable by a generated client as one that does, and was where most of the bare objects were
 * hiding.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class OpenApiContributors {

  /**
   * Every contributor {@code OpenApiSpecGenerator} assembles, built fresh. Named rather than discovered so a new
   * contributor is a deliberate addition to the sweeps rather than something they silently stop covering.
   */
  static List<OpenApiContributor> all() {
    return List.of(new CoreApiSpec(), new AuthApiSpec(), new SecurityAdminApiSpec(), new AiApiSpec(),
        new McpApiSpec(), new PluginApiSpec(), new PrometheusApiSpec(), new GrafanaApiSpec(),
        new TimeSeriesApiSpec(), new VectorApiSpec());
  }

  static OpenAPI contribute(final OpenApiContributor contributor) {
    final OpenAPI openAPI = new OpenAPI();
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    contributor.contribute(openAPI);
    return openAPI;
  }

  /** Every component schema of one contributor, keyed by its component name. */
  static Map<String, Schema> components(final OpenAPI openAPI) {
    final Map<String, Schema> schemas = openAPI.getComponents().getSchemas();
    return schemas == null ? Map.of() : schemas;
  }

  /**
   * Every schema reachable from one contributor's document, keyed by a readable path to it: the component
   * schemas and, under {@code paths.<path>.<method>...}, every body and response schema declared inline.
   */
  static Map<String, Schema<?>> everySchema(final OpenAPI openAPI) {
    final Map<String, Schema<?>> found = new LinkedHashMap<>();

    for (final Map.Entry<String, Schema> entry : components(openAPI).entrySet())
      found.put(entry.getKey(), entry.getValue());

    openAPI.getPaths().forEach((path, pathItem) -> pathItem.readOperationsMap().forEach((method, operation) -> {
      final String prefix = "paths." + path + "." + method.name().toLowerCase();
      if (operation.getRequestBody() != null && operation.getRequestBody().getContent() != null)
        operation.getRequestBody().getContent().forEach(
            (mediaType, media) -> found.put(prefix + ".requestBody[" + mediaType + "]", media.getSchema()));

      if (operation.getResponses() != null)
        operation.getResponses().forEach((status, response) -> {
          if (response.getContent() != null)
            response.getContent().forEach((mediaType, media) ->
                found.put(prefix + ".responses." + status + "[" + mediaType + "]", media.getSchema()));
        });
    }));

    found.values().removeIf(java.util.Objects::isNull);
    return found;
  }

  /**
   * Walks a schema and everything nested in it - properties, array items, typed {@code additionalProperties},
   * and the {@code oneOf}/{@code anyOf}/{@code allOf} branches - calling {@code visitor} with a readable path
   * for each. A {@code $ref} is not followed: the component it names is visited on its own.
   */
  static void walk(final String path, final Schema<?> schema, final SchemaVisitor visitor) {
    if (schema == null || schema.get$ref() != null)
      return;

    visitor.visit(path, schema);

    if (schema.getProperties() != null)
      for (final Map.Entry<String, Schema> entry : schema.getProperties().entrySet())
        walk(path + "." + entry.getKey(), entry.getValue(), visitor);

    if (schema.getItems() != null)
      walk(path + "[]", schema.getItems(), visitor);

    if (schema.getAdditionalProperties() instanceof final Schema<?> values)
      walk(path + ".*", values, visitor);

    walkAll(path + "/oneOf", schema.getOneOf(), visitor);
    walkAll(path + "/anyOf", schema.getAnyOf(), visitor);
    walkAll(path + "/allOf", schema.getAllOf(), visitor);
  }

  private static void walkAll(final String path, final List<Schema> branches, final SchemaVisitor visitor) {
    if (branches == null)
      return;
    final List<Schema> list = new ArrayList<>(branches);
    for (int i = 0; i < list.size(); i++)
      walk(path + "[" + i + "]", list.get(i), visitor);
  }

  @FunctionalInterface
  interface SchemaVisitor {
    void visit(String path, Schema<?> schema);
  }

  private OpenApiContributors() {
  }
}
