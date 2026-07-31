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

import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.parameters.RequestBody;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;
import io.swagger.v3.oas.models.security.SecurityRequirement;

import java.util.List;

/**
 * Shared builders for the OpenAPI contributors. Carries no domain knowledge: every method turns a
 * few literals into one swagger model object, so a contributor reads as a declaration of its
 * endpoints rather than as model plumbing.
 */
public final class SpecBuilders {
  public static final String JSON      = "application/json";
  public static final String ERROR_REF = "ErrorResponse";

  private SpecBuilders() {
  }

  public static Parameter pathParam(final String name, final String description) {
    return pathParam(name, description, null);
  }

  public static Parameter pathParam(final String name, final String description, final List<String> enumValues) {
    final Parameter param = new Parameter();
    param.setName(name);
    param.setIn("path");
    param.setRequired(true);
    param.setDescription(description);
    final Schema<String> schema = new Schema<>();
    schema.setType("string");
    if (enumValues != null)
      schema.setEnum(enumValues);
    param.setSchema(schema);
    return param;
  }

  public static Parameter queryParam(final String name, final String description, final boolean required) {
    return queryParam(name, description, required, "string");
  }

  public static Parameter queryParam(final String name, final String description, final boolean required,
      final String type) {
    final Parameter param = new Parameter();
    param.setName(name);
    param.setIn("query");
    param.setRequired(required);
    param.setDescription(description);
    param.setSchema(new Schema<>().type(type));
    return param;
  }

  /**
   * A query parameter the server reads as a repeated multi-valued key. Declared as an array with
   * form style and explode enabled, which serializes as name=v1&name=v2, the form a repeated
   * parameter actually takes on the wire.
   */
  public static Parameter repeatableQueryParam(final String name, final String description,
      final boolean required) {
    final Parameter param = new Parameter();
    param.setName(name);
    param.setIn("query");
    param.setRequired(required);
    param.setDescription(description);
    param.setStyle(Parameter.StyleEnum.FORM);
    param.setExplode(true);
    param.setSchema(new Schema<>().type("array").items(new Schema<>().type("string")));
    return param;
  }

  public static Schema<Object> object(final String description) {
    final Schema<Object> schema = new Schema<>();
    schema.setType("object");
    schema.setDescription(description);
    return schema;
  }

  public static Schema<String> string(final String description) {
    final Schema<String> schema = new Schema<>();
    schema.setType("string");
    schema.setDescription(description);
    return schema;
  }

  public static Schema<Number> integer(final String description) {
    final Schema<Number> schema = new Schema<>();
    schema.setType("integer");
    schema.setDescription(description);
    return schema;
  }

  public static Schema<Boolean> bool(final String description) {
    final Schema<Boolean> schema = new Schema<>();
    schema.setType("boolean");
    schema.setDescription(description);
    return schema;
  }

  public static Schema<?> arrayOf(final Schema<?> items, final String description) {
    return new Schema<>().type("array").items(items).description(description);
  }

  public static Schema<?> ref(final String componentName) {
    return new Schema<>().$ref("#/components/schemas/" + componentName);
  }

  public static RequestBody jsonBody(final String description, final String componentName, final boolean required) {
    final RequestBody body = new RequestBody();
    body.setDescription(description);
    body.setRequired(required);
    final MediaType mediaType = new MediaType();
    mediaType.setSchema(componentName == null ? new Schema<>().type("object") : ref(componentName));
    body.setContent(new Content().addMediaType(JSON, mediaType));
    return body;
  }

  public static RequestBody rawBody(final String description, final String mediaType, final String format) {
    final RequestBody body = new RequestBody();
    body.setDescription(description);
    body.setRequired(true);
    final MediaType media = new MediaType();
    media.setSchema(new Schema<>().type("string").format(format));
    body.setContent(new Content().addMediaType(mediaType, media));
    return body;
  }

  public static ApiResponse jsonResponse(final String description, final String componentName) {
    final ApiResponse response = new ApiResponse();
    response.setDescription(description);
    final MediaType mediaType = new MediaType();
    mediaType.setSchema(componentName == null ? new Schema<>().type("object") : ref(componentName));
    response.setContent(new Content().addMediaType(JSON, mediaType));
    return response;
  }

  public static ApiResponse emptyResponse(final String description) {
    final ApiResponse response = new ApiResponse();
    response.setDescription(description);
    return response;
  }

  public static ApiResponse errorResponse(final String description) {
    return jsonResponse(description, ERROR_REF);
  }

  /**
   * Builds a response set from one success entry plus the error codes named in {@code extraCodes},
   * each mapped to the standard error body with a description derived from the code.
   */
  public static ApiResponses standardResponses(final String successCode, final ApiResponse success,
      final String... extraCodes) {
    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse(successCode, success);
    for (final String code : extraCodes)
      responses.addApiResponse(code, errorResponse(describeStatus(code)));
    return responses;
  }

  private static String describeStatus(final String code) {
    return switch (code) {
      case "400" -> "Bad request";
      case "401" -> "Unauthorized";
      case "403" -> "Forbidden";
      case "404" -> "Not found";
      case "405" -> "Method not allowed";
      case "408" -> "Request timeout: the body ended before it was fully consumed";
      case "409" -> "Conflict";
      case "413" -> "Request body too large";
      case "500" -> "Internal server error";
      case "503" -> "Service unavailable";
      case "504" -> "Gateway timeout";
      default -> "Error " + code;
    };
  }

  public static Operation operation(final String operationId, final String tag, final String summary,
      final String description) {
    final Operation op = new Operation();
    op.setOperationId(operationId);
    op.addTagsItem(tag);
    op.setSummary(summary);
    op.setDescription(description);
    return op;
  }

  /** Marks an operation as reachable without credentials, overriding the root security declaration. */
  public static void publicOperation(final Operation op) {
    op.setSecurity(List.of());
  }

  /** Restricts an operation to HTTP Basic, for handlers that never reach the bearer-token branch. */
  public static void basicAuthOnly(final Operation op) {
    op.setSecurity(List.of(new SecurityRequirement().addList("basicAuth")));
  }
}
