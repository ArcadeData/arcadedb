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
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;

import java.util.List;

/**
 * Documents time-series ingestion and querying. Ingestion speaks InfluxDB Line Protocol rather than
 * JSON, and the query response takes one of two structurally different shapes depending on whether
 * the request asked for aggregation.
 */
public class TimeSeriesApiSpec implements OpenApiContributor {

  @Override
  public void contribute(final OpenAPI openAPI) {
    openAPI.getPaths().addPathItem("/api/v1/ts/{database}/write", createWritePath());
    openAPI.getPaths().addPathItem("/api/v1/ts/{database}/query", createQueryPath());
    openAPI.getPaths().addPathItem("/api/v1/ts/{database}/latest", createLatestPath());

    openAPI.getComponents().addSchemas("TimeSeriesQueryRequest", createQueryRequestSchema());
    openAPI.getComponents().addSchemas("TimeSeriesRawResponse", createRawResponseSchema());
    openAPI.getComponents().addSchemas("TimeSeriesAggregatedResponse", createAggregatedResponseSchema());
    openAPI.getComponents().addSchemas("TimeSeriesLatestResponse", createLatestResponseSchema());
    openAPI.getComponents().addSchemas("TimeSeriesWriteError", createWriteErrorSchema());
  }

  private PathItem createWritePath() {
    final Operation post = SpecBuilders.operation("writeTimeSeries", "TimeSeries",
        "Ingest samples in InfluxDB Line Protocol",
        """
            Ingests one or more samples expressed in InfluxDB Line Protocol. The measurement name \
            selects the time-series type, tags select the series, and fields carry the values.

            The body may be gzip-compressed by sending Content-Encoding: gzip. A fully accepted \
            request answers 204 with no body; a request whose samples could not all be applied \
            answers 400 with the counts of what was written and dropped, so a client can tell a total \
            rejection from a partial one.""");
    post.addParametersItem(SpecBuilders.pathParam("database", "Database name"));

    final Parameter precision = SpecBuilders.queryParam("precision",
        "Unit of the timestamps in the body. Defaults to nanoseconds when omitted.", false);
    precision.getSchema().setEnum(List.of("ns", "us", "ms", "s"));
    post.addParametersItem(precision);

    post.setRequestBody(SpecBuilders.rawBody(
        "InfluxDB Line Protocol text, one measurement per line. Optionally gzip-compressed.",
        "text/plain", null));

    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("204", SpecBuilders.emptyResponse("All samples ingested"));
    responses.addApiResponse("400",
        SpecBuilders.jsonResponse("Samples rejected, with the counts written and dropped",
            "TimeSeriesWriteError"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("403", SpecBuilders.errorResponse("Forbidden"));
    responses.addApiResponse("404", SpecBuilders.errorResponse("Database not found"));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));
    post.setResponses(responses);

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createQueryPath() {
    final Operation post = SpecBuilders.operation("queryTimeSeries", "TimeSeries",
        "Query samples, optionally aggregated into buckets",
        """
            Reads samples from a time-series type over a timestamp range, optionally filtered by tag \
            and projected to a subset of fields.

            The response shape depends on the request: without 'aggregation' it carries raw rows \
            under 'rows'; with 'aggregation' it carries fixed-interval buckets under 'buckets' and \
            names the computed aggregations under 'aggregations'.""");
    post.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    post.setRequestBody(SpecBuilders.jsonBody("Query definition", "TimeSeriesQueryRequest", true));

    final ApiResponse success = new ApiResponse();
    success.setDescription("Samples, raw or aggregated according to the request");
    final Schema<?> oneOf = new Schema<>();
    oneOf.setOneOf(List.of(//
        SpecBuilders.ref("TimeSeriesRawResponse"), //
        SpecBuilders.ref("TimeSeriesAggregatedResponse")));
    final MediaType mediaType = new MediaType();
    mediaType.setSchema(oneOf);
    success.setContent(new Content().addMediaType(SpecBuilders.JSON, mediaType));

    post.setResponses(SpecBuilders.standardResponses("200", success,
        "400", "401", "403", "404", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createLatestPath() {
    final Operation get = SpecBuilders.operation("getTimeSeriesLatest", "TimeSeries",
        "Read the most recent sample of a series",
        """
            Returns the most recent sample of a time-series type, optionally narrowed to one series \
            by tag. 'latest' is null when the type or the selected series holds no sample.""");
    get.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    get.addParametersItem(SpecBuilders.queryParam("type", "Time-series type name", true));
    get.addParametersItem(SpecBuilders.queryParam("tag",
        "Tag filter in name:value form. Only the first occurrence is honored if the parameter repeats.",
        false));
    get.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Most recent sample", "TimeSeriesLatestResponse"),
        "400", "401", "403", "404", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private Schema<?> createQueryRequestSchema() {
    final Schema<Object> request = SpecBuilders.object("One aggregation to compute over a bucket");
    request.addProperty("field", SpecBuilders.string("Field name to aggregate"));
    request.addProperty("type", SpecBuilders.string(
        "Aggregation function, for example AVG, SUM, MIN, MAX, COUNT"));
    request.addProperty("alias", SpecBuilders.string(
        "Output name. Defaults to the field name suffixed with the lower-cased aggregation type."));

    final Schema<Object> aggregation = SpecBuilders.object(
        "Bucketed aggregation. Present only when the caller wants buckets rather than raw rows.");
    aggregation.addProperty("bucketInterval", SpecBuilders.integer(
        "Bucket width in the same unit as the timestamps"));
    aggregation.addProperty("requests", SpecBuilders.arrayOf(request, "Aggregations to compute"));

    final Schema<Object> schema = SpecBuilders.object("Time-series query definition");
    schema.addProperty("type", SpecBuilders.string("Time-series type name"));
    schema.addProperty("from", SpecBuilders.integer(
        "Inclusive lower bound of the timestamp range. Unbounded when omitted."));
    schema.addProperty("to", SpecBuilders.integer(
        "Inclusive upper bound of the timestamp range. Unbounded when omitted."));
    schema.addProperty("tags", SpecBuilders.object(
        "Tag filter as name to value pairs. All pairs must match."));
    schema.addProperty("fields", SpecBuilders.arrayOf(
        SpecBuilders.string("Field name"), "Fields to project. All fields when omitted."));
    schema.addProperty("aggregation", aggregation);
    schema.addProperty("limit", SpecBuilders.integer(
        "Maximum rows to return for a raw (non-aggregated) query. Defaults to 20000. Ignored when "
            + "'aggregation' is present."));
    schema.setRequired(List.of("type"));
    return schema;
  }

  private Schema<?> createRawResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Raw samples");
    schema.addProperty("type", SpecBuilders.string("Time-series type name"));
    schema.addProperty("columns", SpecBuilders.arrayOf(
        SpecBuilders.string("Column name"), "Column names, in the order the row values appear"));
    schema.addProperty("rows", SpecBuilders.arrayOf(
        SpecBuilders.arrayOf(SpecBuilders.object("Column value"), "One row"),
        "Rows, each positionally aligned with 'columns'"));
    schema.addProperty("count", SpecBuilders.integer("Number of rows returned"));
    return schema;
  }

  private Schema<?> createAggregatedResponseSchema() {
    final Schema<Object> bucket = SpecBuilders.object("One aggregation bucket");
    bucket.addProperty("timestamp", SpecBuilders.integer("Bucket start timestamp"));
    bucket.addProperty("values", SpecBuilders.arrayOf(
        SpecBuilders.object("Aggregated value"),
        "Aggregated values, positionally aligned with 'aggregations'"));

    final Schema<Object> schema = SpecBuilders.object("Aggregated samples");
    schema.addProperty("type", SpecBuilders.string("Time-series type name"));
    schema.addProperty("aggregations", SpecBuilders.arrayOf(
        SpecBuilders.string("Aggregation alias"),
        "Aliases of the computed aggregations, in bucket value order"));
    schema.addProperty("buckets", SpecBuilders.arrayOf(bucket, "Buckets, ordered by timestamp"));
    schema.addProperty("count", SpecBuilders.integer("Number of buckets returned"));
    return schema;
  }

  private Schema<?> createLatestResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Most recent sample of a series");
    schema.addProperty("type", SpecBuilders.string("Time-series type name"));
    schema.addProperty("columns", SpecBuilders.arrayOf(
        SpecBuilders.string("Column name"), "Column names, in sample value order"));
    final Schema<?> latest = SpecBuilders.arrayOf(SpecBuilders.object("Column value"),
        "Most recent sample, positionally aligned with 'columns'. Null when the series is empty.");
    latest.setNullable(true);
    schema.addProperty("latest", latest);
    return schema;
  }

  private Schema<?> createWriteErrorSchema() {
    final Schema<Object> schema = SpecBuilders.object("Rejected ingestion, with partial counts");
    schema.addProperty("error", SpecBuilders.string("Why the request was rejected"));
    schema.addProperty("requestId", SpecBuilders.string(
        "Correlation id echoing X-Request-Id, for matching against server logs"));
    schema.addProperty("written", SpecBuilders.integer("Samples successfully ingested"));
    schema.addProperty("dropped", SpecBuilders.integer("Samples discarded"));
    schema.addProperty("unknownTypes", SpecBuilders.arrayOf(
        SpecBuilders.string("Measurement name"),
        "Measurements naming a type that does not exist"));
    schema.addProperty("nonTimeSeriesTypes", SpecBuilders.arrayOf(
        SpecBuilders.string("Type name"),
        "Measurements naming a type that exists but is not a time-series type"));
    return schema;
  }
}
