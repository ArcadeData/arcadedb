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
 * Documents the Prometheus remote read and write endpoints and the Prometheus-compatible query API.
 * <p>
 * Remote read and write exchange Snappy-compressed protobuf messages, which no JSON schema can
 * describe, so their bodies are declared as opaque binary and the framing lives in the description.
 * The query API answers in the Prometheus HTTP API envelope, which takes four distinct shapes.
 */
public class PrometheusApiSpec implements OpenApiContributor {

  private static final String BASE     = "/api/v1/ts/{database}/prom";
  private static final String PROTOBUF = "application/x-protobuf";

  @Override
  public void contribute(final OpenAPI openAPI) {
    openAPI.getPaths().addPathItem(BASE + "/write", createRemoteWritePath());
    openAPI.getPaths().addPathItem(BASE + "/read", createRemoteReadPath());
    openAPI.getPaths().addPathItem(BASE + "/api/v1/query", createInstantQueryPath());
    openAPI.getPaths().addPathItem(BASE + "/api/v1/query_range", createRangeQueryPath());
    openAPI.getPaths().addPathItem(BASE + "/api/v1/labels", createLabelsPath());
    openAPI.getPaths().addPathItem(BASE + "/api/v1/label/{name}/values", createLabelValuesPath());
    openAPI.getPaths().addPathItem(BASE + "/api/v1/series", createSeriesPath());

    openAPI.getComponents().addSchemas("PromQLDataResponse", createDataResponseSchema());
    openAPI.getComponents().addSchemas("PromQLLabelsResponse", createLabelsResponseSchema());
    openAPI.getComponents().addSchemas("PromQLSeriesResponse", createSeriesResponseSchema());
    openAPI.getComponents().addSchemas("PromQLErrorResponse", createErrorResponseSchema());
  }

  private PathItem createRemoteWritePath() {
    final Operation post = SpecBuilders.operation("prometheusRemoteWrite", "Prometheus",
        "Ingest samples through Prometheus remote write",
        """
            Accepts a Prometheus remote-write request: a protobuf WriteRequest message compressed \
            with Snappy block format. Configure this endpoint as a remote_write target in \
            prometheus.yml. A time series is auto-created from the metric name and labels on first \
            write. Answers 204 with no body once the samples are applied; an empty write request also \
            answers 204.""");
    post.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    post.setRequestBody(SpecBuilders.rawBody(
        "Snappy-compressed protobuf WriteRequest, per the Prometheus remote-write specification",
        PROTOBUF, "binary"));

    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("204", SpecBuilders.emptyResponse("Samples ingested"));
    responses.addApiResponse("400", SpecBuilders.errorResponse(
        "Bad request: database parameter missing, body empty, or not Snappy-compressed"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("403", SpecBuilders.errorResponse("Forbidden"));
    responses.addApiResponse("404", SpecBuilders.errorResponse("Database not found"));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));
    post.setResponses(responses);

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createRemoteReadPath() {
    final Operation post = SpecBuilders.operation("prometheusRemoteRead", "Prometheus",
        "Read samples through Prometheus remote read",
        """
            Accepts a Prometheus remote-read request: a protobuf ReadRequest message compressed with \
            Snappy block format. Answers with a Snappy-compressed protobuf ReadResponse. Configure \
            this endpoint as a remote_read target in prometheus.yml.""");
    post.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    post.setRequestBody(SpecBuilders.rawBody(
        "Snappy-compressed protobuf ReadRequest, per the Prometheus remote-read specification",
        PROTOBUF, "binary"));

    final ApiResponse success = new ApiResponse();
    success.setDescription("Snappy-compressed protobuf ReadResponse");
    final MediaType mediaType = new MediaType();
    mediaType.setSchema(new Schema<>().type("string").format("binary"));
    success.setContent(new Content().addMediaType(PROTOBUF, mediaType));

    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("200", success);
    responses.addApiResponse("400", SpecBuilders.errorResponse(
        "Bad request: database parameter missing, body empty, or not Snappy-compressed"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("403", SpecBuilders.errorResponse("Forbidden"));
    responses.addApiResponse("404", SpecBuilders.errorResponse("Database not found"));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));
    post.setResponses(responses);

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createInstantQueryPath() {
    final Operation get = SpecBuilders.operation("promQLQuery", "PromQL",
        "Evaluate a PromQL expression at a single instant",
        """
            Evaluates a PromQL expression at one point in time. Compatible with the Prometheus \
            /api/v1/query endpoint, so Grafana's Prometheus data source and promtool can target it \
            directly.""");
    get.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    get.addParametersItem(SpecBuilders.queryParam("query", "PromQL expression", true));
    get.addParametersItem(SpecBuilders.queryParam("time",
        "Evaluation instant as a Unix timestamp in seconds, fractional seconds allowed. Defaults to now.",
        false));
    get.addParametersItem(SpecBuilders.queryParam("lookback_delta",
        "How far back to look for a sample, as a duration such as '5m'. Defaults to the server setting.",
        false));
    get.setResponses(promQlResponses("Evaluation result", "PromQLDataResponse"));

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private PathItem createRangeQueryPath() {
    final Operation get = SpecBuilders.operation("promQLQueryRange", "PromQL",
        "Evaluate a PromQL expression over a time range",
        """
            Evaluates a PromQL expression at every step across a range. Compatible with the \
            Prometheus /api/v1/query_range endpoint. 'step' must be positive; a non-positive step \
            answers 400.""");
    get.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    get.addParametersItem(SpecBuilders.queryParam("query", "PromQL expression", true));
    get.addParametersItem(SpecBuilders.queryParam("start",
        "Inclusive range start as a Unix timestamp in seconds, fractional seconds allowed", true));
    get.addParametersItem(SpecBuilders.queryParam("end",
        "Inclusive range end as a Unix timestamp in seconds, fractional seconds allowed", true));
    get.addParametersItem(SpecBuilders.queryParam("step",
        "Evaluation interval, either a plain number of seconds or a duration such as '1m'", true));
    get.addParametersItem(SpecBuilders.queryParam("lookback_delta",
        "How far back to look for a sample, as a duration such as '5m'. Defaults to the server setting.",
        false));
    get.setResponses(promQlResponses("Evaluation result", "PromQLDataResponse"));

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private PathItem createLabelsPath() {
    final Operation get = SpecBuilders.operation("promQLLabels", "PromQL",
        "List label names",
        """
            Lists every label name present in the database, sorted, always including '__name__'. \
            Compatible with the Prometheus /api/v1/labels endpoint. Takes no filtering parameters: \
            unlike Prometheus itself, this endpoint does not accept 'start', 'end', or 'match[]'.""");
    get.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    get.setResponses(promQlResponses("Sorted label names", "PromQLLabelsResponse"));

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private PathItem createLabelValuesPath() {
    final Operation get = SpecBuilders.operation("promQLLabelValues", "PromQL",
        "List the values of one label",
        """
            Lists every value of one label name, sorted. Compatible with the Prometheus \
            /api/v1/label/{name}/values endpoint. Querying '__name__' returns every time-series type \
            name instead of scanning a tag column. Takes no filtering parameters: unlike Prometheus \
            itself, this endpoint does not accept 'start', 'end', or 'match[]'.""");
    get.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    get.addParametersItem(SpecBuilders.pathParam("name", "Label name"));
    get.setResponses(promQlResponses("Sorted label values", "PromQLLabelsResponse"));

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private PathItem createSeriesPath() {
    final Operation get = SpecBuilders.operation("promQLSeries", "PromQL",
        "Find series matching selectors",
        """
            Returns the label sets of the series matching the given selectors. Compatible with the \
            Prometheus /api/v1/series endpoint. Each returned object is a label map including the \
            '__name__' label. A selector that fails to parse is skipped rather than rejected, so a \
            mix of valid and malformed 'match[]' values still returns the matches from the valid ones.""");
    get.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    final Parameter match = SpecBuilders.repeatableQueryParam("match[]",
        "Series selector. Repeatable: every occurrence is evaluated and the results are unioned.", true);
    get.addParametersItem(match);
    get.addParametersItem(SpecBuilders.queryParam("start",
        "Inclusive range start as a Unix timestamp in seconds, fractional seconds allowed", false));
    get.addParametersItem(SpecBuilders.queryParam("end",
        "Inclusive range end as a Unix timestamp in seconds, fractional seconds allowed", false));
    get.setResponses(promQlResponses("Matching series label sets", "PromQLSeriesResponse"));

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  /**
   * The Prometheus API reports failures in its own envelope rather than the ArcadeDB error body, so
   * a client written against the Prometheus API can parse both outcomes with one reader.
   */
  private ApiResponses promQlResponses(final String successDescription, final String successSchema) {
    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("200", SpecBuilders.jsonResponse(successDescription, successSchema));
    responses.addApiResponse("400",
        SpecBuilders.jsonResponse("Bad request, in the Prometheus error envelope", "PromQLErrorResponse"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("403", SpecBuilders.errorResponse("Forbidden"));
    responses.addApiResponse("404", SpecBuilders.errorResponse("Database not found"));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));
    return responses;
  }

  private Schema<?> createDataResponseSchema() {
    final Schema<String> resultType = SpecBuilders.string(
        "Shape of 'result': a vector of instant samples, a matrix of range samples, or a scalar");
    resultType.setEnum(List.of("vector", "matrix", "scalar"));

    final Schema<Object> vectorEntry = SpecBuilders.object("One instant sample");
    vectorEntry.addProperty("metric", metricLabelsSchema());
    vectorEntry.addProperty("value", samplePair());
    vectorEntry.setRequired(List.of("metric", "value"));

    final Schema<Object> matrixEntry = SpecBuilders.object("One range series");
    matrixEntry.addProperty("metric", metricLabelsSchema());
    matrixEntry.addProperty("values", SpecBuilders.arrayOf(samplePair(), "Samples ordered by timestamp"));
    matrixEntry.setRequired(List.of("metric", "values"));

    final Schema<Object> result = new Schema<>();
    result.setDescription("""
        Evaluation result, shaped by 'resultType': an array of instant samples when 'vector', an \
        array of range series when 'matrix', and a single [timestamp, value] pair when 'scalar'.""");
    // anyOf, not oneOf: an empty 'result' array (a normal empty vector or matrix result) validates
    // against every branch here, so oneOf's "exactly one match" can never be satisfied. 'resultType'
    // remains the real narrowing key for a human, or a hand-written narrowing function.
    result.setAnyOf(List.of(
        SpecBuilders.arrayOf(vectorEntry, "Entries when resultType is 'vector'"),
        SpecBuilders.arrayOf(matrixEntry, "Entries when resultType is 'matrix'"),
        samplePair()));

    final Schema<Object> data = SpecBuilders.object("Evaluation result");
    data.addProperty("resultType", resultType);
    data.addProperty("result", result);

    final Schema<Object> schema = SpecBuilders.object("Prometheus query response");
    schema.addProperty("status", SpecBuilders.string("Always 'success' on a 200"));
    schema.addProperty("data", data);
    return schema;
  }

  /**
   * A fresh [timestamp, value] pair schema on every call. Returning a shared instance would place
   * one mutable schema object at several points in the document, the same defect class that put one
   * ApiResponse under two status keys during #4895.
   * <p>
   * The server actually sends a 2-element JSON array of a number then a string (see
   * {@code PromQLResponseFormatter}), neither of which is an object, so the items schema is left
   * unconstrained rather than typed as 'object'. The tuple shape is instead pinned with
   * minItems/maxItems.
   */
  private Schema<?> samplePair() {
    final Schema<?> pair = SpecBuilders.arrayOf(new Schema<>(),
        "One [timestamp, value] pair: a Unix timestamp in seconds (number), then the sample value (string)");
    pair.setMinItems(2);
    pair.setMaxItems(2);
    return pair;
  }

  /**
   * The PromQL label map is free-form (label names are not known ahead of time), but every value
   * {@code labelsToJson} writes is a JSON string, so 'additionalProperties' is pinned to a string
   * schema rather than left as bare 'object'. That is what makes a generator emit Map&lt;String,
   * String&gt; instead of Map&lt;String, Object&gt;.
   */
  private Schema<Object> metricLabelsSchema() {
    final Schema<Object> schema = SpecBuilders.object("Label map, including the '__name__' label");
    schema.setAdditionalProperties(SpecBuilders.string("Label value"));
    return schema;
  }

  private Schema<?> createLabelsResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Prometheus label response");
    schema.addProperty("status", SpecBuilders.string("Always 'success' on a 200"));
    schema.addProperty("data", SpecBuilders.arrayOf(
        SpecBuilders.string("Label name or value"), "Sorted names or values"));
    return schema;
  }

  private Schema<?> createSeriesResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Prometheus series response");
    schema.addProperty("status", SpecBuilders.string("Always 'success' on a 200"));
    schema.addProperty("data", SpecBuilders.arrayOf(
        SpecBuilders.object("One series as a label map, including the '__name__' label"),
        "Matching series"));
    return schema;
  }

  private Schema<?> createErrorResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Prometheus error envelope");
    schema.addProperty("status", SpecBuilders.string("Always 'error'"));
    schema.addProperty("errorType", SpecBuilders.string(
        "Prometheus error class, for example 'bad_data'"));
    schema.addProperty("error", SpecBuilders.string("Human-readable message"));
    return schema;
  }
}
