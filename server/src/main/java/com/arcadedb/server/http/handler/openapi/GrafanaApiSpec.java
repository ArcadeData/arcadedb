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

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.responses.ApiResponse;

import java.util.List;

/**
 * Documents the Grafana data source endpoints. Query results use the Grafana DataFrame envelope:
 * one entry per request refId, each holding frames whose schema names the fields and whose data
 * holds one column-major array per field.
 * <p>
 * All three operations moved onto {@code DatabaseAbstractHandler} in issue #7681, so they honour the session
 * header the way {@code /api/v1/query} and the {@code /api/v1/ts} operations of issue #7402 do. Until then the
 * header was accepted by the transport and ignored by the handler, and the document said nothing either way -
 * which reads to a client generator as "this route has nothing to do with transactions" rather than as the gap
 * it was. All three are reads, so a session id that no longer resolves degrades rather than being refused.
 */
public class GrafanaApiSpec implements OpenApiContributor {

  @Override
  public void contribute(final OpenAPI openAPI) {
    openAPI.getPaths().addPathItem("/api/v1/ts/{database}/grafana/health", createHealthPath());
    openAPI.getPaths().addPathItem("/api/v1/ts/{database}/grafana/metadata", createMetadataPath());
    openAPI.getPaths().addPathItem("/api/v1/ts/{database}/grafana/query", createQueryPath());

    openAPI.getComponents().addSchemas("GrafanaHealth", createHealthSchema());
    openAPI.getComponents().addSchemas("GrafanaMetadata", createMetadataSchema());
    openAPI.getComponents().addSchemas("GrafanaQueryRequest", createQueryRequestSchema());
    openAPI.getComponents().addSchemas("GrafanaQueryResponse", createQueryResponseSchema());
  }

  private PathItem createHealthPath() {
    final Operation get = SpecBuilders.operation("checkGrafanaHealth", "Grafana",
        "Test the data source connection",
        "Answers the Grafana data source health check for one database.");
    get.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    get.addParametersItem(SpecBuilders.sessionHeaderParam());

    final ApiResponse health = SpecBuilders.jsonResponse("Data source reachable", "GrafanaHealth");
    health.addHeaderObject(SpecBuilders.SESSION_HEADER, SpecBuilders.sessionEchoHeader());
    get.setResponses(SpecBuilders.standardResponses("200", health, "400", "401", "403", "404", "500"));
    get.getResponses().addApiResponse("404",
        SpecBuilders.errorResponse(SpecBuilders.READ_STALE_SESSION_DESCRIPTION));

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private PathItem createMetadataPath() {
    final Operation get = SpecBuilders.operation("getGrafanaMetadata", "Grafana",
        "List queryable types, fields, and tags",
        """
            Describes what a Grafana panel can query: the time-series types in the database, each \
            with its value fields and its tag fields (both carrying a name and a data type), and \
            the aggregation functions the server supports.""");
    get.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    get.addParametersItem(SpecBuilders.sessionHeaderParam());

    // Worth presenting here more than anywhere else on this prefix: what this operation reports is the SCHEMA,
    // and a type created inside the caller's open transaction is visible to that transaction and to nothing
    // else. Answered from outside the session, a panel that had just created a type was told it does not exist.
    final ApiResponse metadata = SpecBuilders.jsonResponse("Queryable metadata", "GrafanaMetadata");
    metadata.addHeaderObject(SpecBuilders.SESSION_HEADER, SpecBuilders.sessionEchoHeader());
    get.setResponses(SpecBuilders.standardResponses("200", metadata, "400", "401", "403", "404", "500"));
    get.getResponses().addApiResponse("404",
        SpecBuilders.errorResponse(SpecBuilders.READ_STALE_SESSION_DESCRIPTION));

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private PathItem createQueryPath() {
    final Operation post = SpecBuilders.operation("queryGrafana", "Grafana",
        "Execute panel queries and return DataFrames",
        """
            Executes one query per entry in 'targets' and returns the results keyed by each target's \
            refId, in the Grafana DataFrame format. A target carrying 'aggregation' produces bucketed \
            values; a target without it produces raw samples, optionally projected to 'fields'. A \
            target naming a missing type, a non-time-series type, or an unresolvable aggregation \
            field gets an 'error' entry with no frames instead of failing the whole request. \
            'maxDataPoints' helps derive a bucket interval when 'aggregation.bucketInterval' is \
            omitted.""");
    post.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    post.addParametersItem(SpecBuilders.sessionHeaderParam());
    post.setRequestBody(SpecBuilders.jsonBody("Grafana panel query", "GrafanaQueryRequest", true));

    final ApiResponse frames = SpecBuilders.jsonResponse("DataFrames keyed by target refId", "GrafanaQueryResponse");
    frames.addHeaderObject(SpecBuilders.SESSION_HEADER, SpecBuilders.sessionEchoHeader());
    post.setResponses(SpecBuilders.standardResponses("200", frames, "400", "401", "403", "404", "500"));
    post.getResponses().addApiResponse("404",
        SpecBuilders.errorResponse(SpecBuilders.READ_STALE_SESSION_DESCRIPTION));

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private Schema<?> createHealthSchema() {
    final Schema<Object> schema = SpecBuilders.object("Data source health");
    schema.addProperty("status", SpecBuilders.string("Always 'ok' when the database is reachable"));
    schema.addProperty("database", SpecBuilders.string("Database the check ran against"));
    return schema;
  }

  private Schema<?> createMetadataSchema() {
    final Schema<Object> field = SpecBuilders.object("One column, value or tag");
    field.addProperty("name", SpecBuilders.string("Column name"));
    field.addProperty("dataType", SpecBuilders.string("ArcadeDB column data type"));

    final Schema<Object> type = SpecBuilders.object("One queryable time-series type");
    type.addProperty("name", SpecBuilders.string("Type name"));
    type.addProperty("fields", SpecBuilders.arrayOf(field, "Value columns"));
    type.addProperty("tags", SpecBuilders.arrayOf(field, "Tag columns available as filters"));

    final Schema<Object> schema = SpecBuilders.object("Queryable metadata");
    schema.addProperty("types", SpecBuilders.arrayOf(type, "Queryable time-series types"));
    schema.addProperty("aggregationTypes", SpecBuilders.arrayOf(
        SpecBuilders.string("Aggregation function name"), "Supported aggregation functions"));
    return schema;
  }

  private Schema<?> createQueryRequestSchema() {
    final Schema<Object> aggregationRequest = SpecBuilders.object("One aggregation to compute");
    aggregationRequest.addProperty("field", SpecBuilders.string("Field name to aggregate"));
    aggregationRequest.addProperty("type", SpecBuilders.string(
        "Aggregation function. Required, one of SUM, AVG, MIN, MAX, COUNT, matched case-insensitively. "
            + "A value that matches none is reported as an error frame on this target, leaving the other "
            + "targets served."));
    aggregationRequest.addProperty("alias", SpecBuilders.string(
        "Output field name. Defaults to the field name suffixed with the lower-cased aggregation type."));

    final Schema<Object> aggregation = SpecBuilders.object(
        "Bucketed aggregation. Omit for raw samples.");
    aggregation.addProperty("bucketInterval", SpecBuilders.integer(
        "Bucket width in the same unit as the timestamps. Derived from 'maxDataPoints' and the "
            + "time range when omitted. When stated it must be positive: a value of zero or less is refused "
            + "with an error frame for this target rather than replaced by a derived interval."));
    aggregation.addProperty("requests", SpecBuilders.arrayOf(
        aggregationRequest,
        "Aggregations to compute. Must name at least one; an empty array is refused with an error frame."));

    final Schema<Object> target = SpecBuilders.object("One panel query");
    target.addProperty("refId", SpecBuilders.string(
        "Identifier echoed back as the result key. Defaults to 'A'."));
    target.addProperty("type", SpecBuilders.string("Time-series type name"));
    target.addProperty("tags", SpecBuilders.object("Tag filter as name to value pairs"));
    target.addProperty("aggregation", aggregation);
    target.addProperty("fields", SpecBuilders.arrayOf(
        SpecBuilders.string("Field name"),
        "Fields to project on a raw (non-aggregated) query. All fields when omitted. Ignored when "
            + "'aggregation' is present. A name that is no column of the type is refused with an error frame "
            + "for this target rather than ignored."));

    final Schema<Object> schema = SpecBuilders.object("Grafana panel query");
    schema.addProperty("targets", SpecBuilders.arrayOf(target, "Queries to execute"));
    schema.addProperty("from", SpecBuilders.integer(
        "Inclusive lower bound of the timestamp range. Unbounded when omitted."));
    schema.addProperty("to", SpecBuilders.integer(
        "Inclusive upper bound of the timestamp range. Unbounded when omitted."));
    schema.addProperty("maxDataPoints", SpecBuilders.integer(
        "Used with the time range to derive a bucket interval when 'aggregation.bucketInterval' "
            + "is omitted."));
    schema.setRequired(List.of("targets"));
    return schema;
  }

  private Schema<?> createQueryResponseSchema() {
    final Schema<Object> frameField = SpecBuilders.object("One frame field");
    frameField.addProperty("name", SpecBuilders.string("Field name, 'time' for the time column"));
    frameField.addProperty("type", SpecBuilders.string("Grafana field type, for example time or number"));

    final Schema<Object> frameSchema = SpecBuilders.object("Frame schema");
    frameSchema.addProperty("fields", SpecBuilders.arrayOf(
        frameField, "Fields, positionally aligned with the value arrays"));

    final Schema<Object> frameData = SpecBuilders.object("Frame data");
    frameData.addProperty("values", SpecBuilders.arrayOf(
        SpecBuilders.arrayOf(SpecBuilders.object("Value"), "One column of values"),
        "Column-major values, one array per field"));

    final Schema<Object> frame = SpecBuilders.object("One DataFrame");
    frame.addProperty("schema", frameSchema);
    frame.addProperty("data", frameData);

    final Schema<Object> perTarget = SpecBuilders.object(
        "Result for one target. Carries 'error' instead of frames when the target could not be "
            + "resolved.");
    perTarget.addProperty("frames", SpecBuilders.arrayOf(frame, "Frames produced by the target"));
    perTarget.addProperty("error", SpecBuilders.string(
        "Why the target could not be resolved. Present only when it failed; 'frames' is then empty."));

    final Schema<Object> results = SpecBuilders.object("Results keyed by target refId");
    results.setAdditionalProperties(perTarget);

    final Schema<Object> schema = SpecBuilders.object("Grafana DataFrame response");
    schema.addProperty("results", results);
    return schema;
  }
}
