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

import com.arcadedb.server.http.HttpSessionManager;
import com.arcadedb.server.http.handler.DatabaseAbstractHandler;
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
  private static final String SESSION_HEADER = HttpSessionManager.ARCADEDB_SESSION_ID;

  // All three operations moved onto DatabaseAbstractHandler in issue #7402, so they honour the session header
  // the way /api/v1/query and /api/v1/command do. Until then the header was accepted by the transport and
  // ignored by the handler, and the document said nothing either way - which reads to a client generator as
  // "this route has nothing to do with transactions" rather than as the gap it was.
  private static final String SESSION_REQUEST_DESCRIPTION = """
      Session id returned by 'beginTransaction'. Present it to run this call inside that transaction: the \
      call then runs under the session's lock and principal and refreshes its idle timer. Omit it to run \
      outside any transaction.""";

  private static final String SESSION_RESPONSE_DESCRIPTION =
      "Echo of the session id this call ran inside. Absent when the call ran outside a transaction.";

  // The read routes override requiresTransaction() to false and so reach DatabaseAbstractHandler's degrading
  // branch, exactly as GET /query does; the write route answers false there but true to
  // rejectsUnresolvableSession(), so it is the one /api/v1/ts operation whose 404 covers the stale-session
  // case. The quoted text is the message AbstractServerHttpHandler actually sends.
  private static final String READ_STALE_SESSION_DESCRIPTION =
      "Database not found. A session id that no longer resolves is NOT an error here: the read degrades to "
          + "running outside the transaction and still answers 200, reporting the degrade in the "
          + DatabaseAbstractHandler.SESSION_EXPIRED + " response header.";

  // Issue #7714: the two protocols answer an unresolvable transaction on a time-series READ differently, and
  // that is a CONTRACT rather than an accident, so it is written down on both surfaces - here, and in the
  // comments on TimeSeriesQuery/TimeSeriesLatest in arcadedb.proto. HTTP follows GET /api/v1/query, whose
  // degrade is what keeps read-after-commit and idempotent retries working; gRPC follows lookupByRid,
  // updateRecord and the search RPCs of #7326, which all answer FAILED_PRECONDITION. Refusing on HTTP would
  // turn a currently-succeeding retry into an error for every client that does one, which is a compatibility
  // break needing a deprecation story rather than a patch. What the degrade may not be is SILENT, which is
  // what the header below is for.
  private static final String SESSION_EXPIRED_RESPONSE_DESCRIPTION =
      "Present only when the request named a session id this server could not resolve (committed, rolled back, "
          + "expired, or owned by another principal). It carries that id, and says this answer was produced "
          + "OUTSIDE the transaction the caller named rather than inside it. The read is not refused, which is "
          + "what keeps a read-after-commit working; the gRPC TimeSeriesQuery/TimeSeriesLatest RPCs refuse the "
          + "same case with FAILED_PRECONDITION, following their own protocol's convention.";

  private static final String WRITE_STALE_SESSION_DESCRIPTION =
      "Database not found, or the session id header names a transaction that no longer resolves "
          + "(\"Remote transaction session not found or expired\"): a write is refused rather than run outside "
          + "the transaction the caller believes it is inside.";

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
    post.addParametersItem(SpecBuilders.headerParam(SESSION_HEADER, SESSION_REQUEST_DESCRIPTION, false));

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
    responses.addApiResponse("404", SpecBuilders.errorResponse(WRITE_STALE_SESSION_DESCRIPTION));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));
    responses.get("204").addHeaderObject(SESSION_HEADER, SpecBuilders.stringHeader(SESSION_RESPONSE_DESCRIPTION));
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
    post.addParametersItem(SpecBuilders.headerParam(SESSION_HEADER, SESSION_REQUEST_DESCRIPTION, false));
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
    success.addHeaderObject(SESSION_HEADER, SpecBuilders.stringHeader(SESSION_RESPONSE_DESCRIPTION));
    success.addHeaderObject(DatabaseAbstractHandler.SESSION_EXPIRED,
        SpecBuilders.stringHeader(SESSION_EXPIRED_RESPONSE_DESCRIPTION));

    post.setResponses(SpecBuilders.standardResponses("200", success,
        "400", "401", "403", "404", "500"));
    post.getResponses().addApiResponse("404", SpecBuilders.errorResponse(READ_STALE_SESSION_DESCRIPTION));
    // The generic "Bad request" text is replaced: the one refusal a caller of this endpoint is most likely to
    // meet is a mistyped tag name, and until #7334 it was not a refusal at all - the term was dropped and the
    // query silently widened to the whole range.
    post.getResponses().addApiResponse("400", SpecBuilders.errorResponse(
        "Bad request. A name in 'tags' that is no TAG column of the type is refused here, naming it and listing "
            + "the type's declared TAG columns: dropping it would widen the query to the whole range, which is "
            + "indistinguishable from a filter that matched everything."));
    // Added explicitly rather than through standardResponses, whose 413 text describes an oversized REQUEST
    // body: here it is the response that would be too large (issue #5719). Both shapes can raise it - the raw
    // one on its rows, the aggregated one on its buckets.
    post.getResponses().addApiResponse("413", SpecBuilders.errorResponse(
        "The rows or buckets exceed 'arcadedb.server.httpQueryMaxResultRows': narrow the range, "
            + "widen 'bucketInterval', or page the query"));

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createLatestPath() {
    final Operation get = SpecBuilders.operation("getTimeSeriesLatest", "TimeSeries",
        "Read the most recent sample of a series",
        """
            Returns the most recent sample of a time-series type, optionally narrowed to one series \
            by tag. Repeat 'tag' once per tag column to name a single series on a type that carries \
            several. 'latest' is null when the type or the selected series holds no sample.""");
    get.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    get.addParametersItem(SpecBuilders.headerParam(SESSION_HEADER, SESSION_REQUEST_DESCRIPTION, false));
    get.addParametersItem(SpecBuilders.queryParam("type", "Time-series type name", true));
    // Repeatable since issue #7321: the handler conjoins every occurrence, the way the query endpoint
    // conjoins the pairs of its 'tags' object, so a plain string parameter would understate the contract
    // and hold generated clients down to one tag.
    get.addParametersItem(SpecBuilders.repeatableQueryParam("tag",
        "Tag filter in name:value form. Repeat the parameter to narrow to one series across several tags: "
            + "every occurrence must match. An occurrence that carries no ':' separator, or whose name is no "
            + "TAG column of the type, is refused with 400 rather than ignored.",
        false));
    final ApiResponse latest = SpecBuilders.jsonResponse("Most recent sample", "TimeSeriesLatestResponse");
    latest.addHeaderObject(SESSION_HEADER, SpecBuilders.stringHeader(SESSION_RESPONSE_DESCRIPTION));
    latest.addHeaderObject(DatabaseAbstractHandler.SESSION_EXPIRED,
        SpecBuilders.stringHeader(SESSION_EXPIRED_RESPONSE_DESCRIPTION));
    get.setResponses(SpecBuilders.standardResponses("200", latest, "400", "401", "403", "404", "500"));
    get.getResponses().addApiResponse("404", SpecBuilders.errorResponse(READ_STALE_SESSION_DESCRIPTION));
    // See the query endpoint: dropping an unresolvable tag is worse here, because this endpoint answers ONE
    // row, so the caller gets the newest sample of some other series rather than a widened result set they
    // could at least inspect (issue #7334).
    get.getResponses().addApiResponse("400", SpecBuilders.errorResponse(
        "Bad request. A 'tag' occurrence not in 'name:value' form, or whose name is no TAG column of the type, "
            + "is refused here, naming it and listing the type's declared TAG columns."));

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private Schema<?> createQueryRequestSchema() {
    final Schema<Object> request = SpecBuilders.object("One aggregation to compute over a bucket");
    request.addProperty("field", SpecBuilders.string("Field name to aggregate"));
    request.addProperty("type", SpecBuilders.string(
        "Aggregation function. Required, one of SUM, AVG, MIN, MAX, COUNT, matched case-insensitively."));
    request.addProperty("alias", SpecBuilders.string(
        "Output name. Defaults to the field name suffixed with the lower-cased aggregation type."));

    final Schema<Object> aggregation = SpecBuilders.object(
        "Bucketed aggregation. Present only when the caller wants buckets rather than raw rows.");
    aggregation.addProperty("bucketInterval", SpecBuilders.integer(
        "Bucket width in the same unit as the timestamps. Must be a positive WHOLE number: a value with a "
            + "fractional part is refused with 400 rather than truncated, because a bucket width is exactly the "
            + "sort of value a client computes by division."));
    aggregation.addProperty("requests", SpecBuilders.arrayOf(request, "Aggregations to compute"));

    final Schema<Object> schema = SpecBuilders.object("Time-series query definition");
    schema.addProperty("type", SpecBuilders.string("Time-series type name"));
    schema.addProperty("from", SpecBuilders.integer(
        "Inclusive lower bound of the timestamp range. Unbounded when omitted."));
    schema.addProperty("to", SpecBuilders.integer(
        "Inclusive upper bound of the timestamp range. Unbounded when omitted."));
    schema.addProperty("tags", SpecBuilders.object(
        "Tag filter as name to value pairs. All pairs must match. A name that is no TAG column of the type is "
            + "refused with 400 rather than ignored."));
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
    schema.addProperty("unavailableTypes", SpecBuilders.arrayOf(
        SpecBuilders.string("Type name"),
        "Measurements naming a time-series type whose storage engine failed to load; see the server log for why"));
    return schema;
  }
}
