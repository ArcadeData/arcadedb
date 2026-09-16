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

import com.arcadedb.engine.timeseries.AggregationType;
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.TagFilter;
import com.arcadedb.engine.timeseries.TimeSeriesGateway;
import com.arcadedb.engine.timeseries.TimeSeriesGateway.TypeResolution;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONException;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.StringJoiner;

/**
 * Adapts the JSON request shapes of the TimeSeries HTTP handlers onto {@link TimeSeriesGateway}, which holds the
 * protocol-neutral semantics shared with the gRPC {@code TimeSeries*} RPCs (issue #7305). Nothing here decides
 * anything: it only turns {@code JSONObject}/{@code JSONArray} into the plain Java shapes the gateway takes, so
 * a change to how a tag filter or a projection is resolved lands on both protocols at once.
 */
final class TimeSeriesHandlerUtils {

  /**
   * Longest caller-supplied value echoed back in the refusal below. The payload is already bounded by the HTTP
   * body-size limit, so this only keeps a long-but-legal string out of an error body that exists to be read.
   */
  private static final int MAX_ECHOED_VALUE_LENGTH = 64;

  private TimeSeriesHandlerUtils() {
  }

  /**
   * Resolves a member that must be present and must be a JSON object, naming it by its full request path when it
   * is not (issue #7340).
   * <p>
   * The whole {@code require*}/{@code opt*} family below exists for one reason: {@link JSONObject}'s raising
   * getters signal an absent, null or wrongly-typed member with a {@link JSONException}, which
   * {@code AbstractServerHttpHandler}'s mapper answers as {@code 400 "Invalid JSON payload"} with the specifics in
   * the {@code detail} field - and {@code buildErrorBody} CONCEALS {@code detail} whenever the server runs in
   * production mode. A caller that omitted {@code field} was told only that its payload was invalid, not which
   * member was missing. These helpers translate the same failures into an {@link IllegalArgumentException} the
   * two endpoints render on the surface they answer errors on: a 400 whose {@code error} field is always sent, or
   * a per-target Grafana error frame. That is the shape #7325 introduced for {@code type}, applied to the members
   * it left out of scope.
   * <p>
   * Each helper DELEGATES to the matching {@link JSONObject} getter rather than re-deciding what is acceptable, so
   * what the endpoints accept is unchanged and only the refusal differs. In particular {@code getString} still
   * renders a JSON number as its text and {@code getLong} still parses a numeric string; widening or narrowing
   * that here would change which requests succeed, which this issue does not ask for.
   *
   * @param path the member's dotted path as the caller wrote it, e.g. {@code targets[0].aggregation}
   */
  static JSONObject requireObject(final JSONObject owner, final String name, final String path) {
    if (owner.isNull(name))
      throw missingMember(path, "a JSON object");
    try {
      return owner.getJSONObject(name);
    } catch (final JSONException e) {
      throw wrongType(path, "a JSON object", owner.opt(name), e);
    }
  }

  /**
   * Resolves a member that must be present and must be a JSON array. See {@link #requireObject}.
   */
  static JSONArray requireArray(final JSONObject owner, final String name, final String path) {
    if (owner.isNull(name))
      throw missingMember(path, "a JSON array");
    try {
      return owner.getJSONArray(name);
    } catch (final JSONException e) {
      throw wrongType(path, "a JSON array", owner.opt(name), e);
    }
  }

  /**
   * Resolves a member that must be present and must be a string. See {@link #requireObject}.
   */
  static String requireString(final JSONObject owner, final String name, final String path) {
    if (owner.isNull(name))
      throw missingMember(path, "a string");
    try {
      return owner.getString(name);
    } catch (final JSONException e) {
      throw wrongType(path, "a string", owner.opt(name), e);
    }
  }

  /**
   * Resolves a member that must be present and must be a number. See {@link #requireObject}.
   */
  static long requireLong(final JSONObject owner, final String name, final String path) {
    if (owner.isNull(name))
      throw missingMember(path, "a number");
    return readLong(owner, name, path);
  }

  /**
   * Resolves an optional member that must be a number when it IS present. An absent or JSON-null member yields
   * {@code defaultValue}; one that arrives as something a number cannot be read from is a client error, refused by
   * name rather than through the concealed {@code detail} field.
   */
  static long optLong(final JSONObject owner, final String name, final long defaultValue, final String path) {
    if (owner.isNull(name))
      return defaultValue;
    return readLong(owner, name, path);
  }

  /**
   * Resolves an optional member that must be an integer when it IS present. See {@link #optLong}.
   * <p>
   * Read as a long and range-checked rather than through {@code JSONObject.getInt}, which narrows with
   * {@code Number.intValue()}: a value an int cannot hold would WRAP instead of being refused, and the caller
   * would silently get a different number than it sent. That is the same trap
   * {@code AbstractServerHttpHandler.requireIntLimit} exists for on the 'limit' member.
   */
  static int optInt(final JSONObject owner, final String name, final int defaultValue, final String path) {
    if (owner.isNull(name))
      return defaultValue;

    final long value = readLong(owner, name, path);
    if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE)
      throw new IllegalArgumentException("'" + path + "' must be an integer between " + Integer.MIN_VALUE + " and "
          + Integer.MAX_VALUE + ": received " + value);

    return (int) value;
  }

  /**
   * Resolves an optional member that must be a string when it IS present. See {@link #optLong}.
   */
  static String optString(final JSONObject owner, final String name, final String defaultValue, final String path) {
    if (owner.isNull(name))
      return defaultValue;
    try {
      return owner.getString(name);
    } catch (final JSONException e) {
      throw wrongType(path, "a string", owner.opt(name), e);
    }
  }

  /**
   * Resolves an array element that must be a JSON object, naming it by its indexed request path.
   *
   * @param index must be within {@code array}'s bounds, which every caller guarantees by iterating over
   *              {@link JSONArray#length()}
   */
  static JSONObject requireObjectElement(final JSONArray array, final int index, final String path) {
    if (array.isNull(index))
      throw missingMember(path, "a JSON object");
    try {
      return array.getJSONObject(index);
    } catch (final JSONException e) {
      throw wrongType(path, "a JSON object", array.get(index), e);
    }
  }

  /**
   * Resolves an array element that must be a string. See {@link #requireObjectElement}.
   */
  static String requireStringElement(final JSONArray array, final int index, final String path) {
    if (array.isNull(index))
      throw missingMember(path, "a string");
    try {
      return array.getString(index);
    } catch (final JSONException e) {
      throw wrongType(path, "a string", array.get(index), e);
    }
  }

  private static long readLong(final JSONObject owner, final String name, final String path) {
    try {
      return owner.getLong(name);
    } catch (final JSONException e) {
      throw wrongType(path, "a number", owner.opt(name), e);
    }
  }

  static IllegalArgumentException missingMember(final String path, final String kind) {
    return new IllegalArgumentException("'" + path + "' is required and must be " + kind);
  }

  private static IllegalArgumentException wrongType(final String path, final String kind, final Object received,
      final Throwable cause) {
    return new IllegalArgumentException("'" + path + "' must be " + kind + ": received " + describe(received), cause);
  }

  /**
   * Renders what arrived for an error message. A container is reported by KIND rather than by content, so a
   * refusal cannot echo a multi-megabyte payload back at the caller, and a primitive is truncated for the same
   * reason.
   */
  private static String describe(final Object received) {
    if (received instanceof JSONObject)
      return "a JSON object";
    if (received instanceof JSONArray)
      return "a JSON array";
    if (received instanceof String text)
      return "'" + truncate(text) + "'";
    return truncate(String.valueOf(received));
  }

  /**
   * Resolves the aggregation function named by one {@code aggregation.requests[]} entry, matching the
   * {@link AggregationType} names case-insensitively after trimming.
   * <p>
   * Both time-series HTTP endpoints route through this so the same bad request gets the same answer on either.
   * It exists because {@code AggregationType.valueOf} is exact: a lower-cased {@code avg} - the spelling a
   * hand-written client is most likely to send - used to throw an {@link IllegalArgumentException} the generic
   * handler mapper renders as "Cannot execute command", with the JVM's own "No enum constant ..." text in the
   * {@code detail} field that {@code buildErrorBody} conceals in production mode. Accepting the lower-cased
   * spelling only widens what is accepted, so no request that used to work stops working, and it matches the SQL
   * push-down planner, which has always matched the same five functions on a lower-cased function name.
   * <p>
   * The message is built from {@link AggregationType#values()} rather than a literal list so a new constant cannot
   * leave it stale. The caller renders it on whatever surface that endpoint answers errors on - a 400 body or a
   * Grafana error frame - which is why this signals with an exception rather than choosing one itself.
   *
   * @param request the aggregation request object, expected to carry a {@code type} member
   * @param index   position of this entry in {@code aggregation.requests}, used to name the field in the message
   *
   * @throws IllegalArgumentException if {@code type} is absent, null, not a string, or matches no aggregation type
   */
  static AggregationType resolveAggregationType(final JSONObject request, final int index) {
    return resolveAggregationType(request, "aggregation.requests[" + index + "].type");
  }

  /**
   * As {@link #resolveAggregationType(JSONObject, int)}, for the endpoint whose requests are nested under a target
   * and whose members are therefore named by a longer path, e.g. {@code targets[0].aggregation.requests[0].type}
   * (issue #7340). The path a caller reads has to be the one it can look up in its own payload.
   */
  static AggregationType resolveAggregationType(final JSONObject request, final String path) {
    final Object rawType = request.opt("type");
    if (rawType instanceof String name) {
      final String trimmed = name.trim();
      if (!trimmed.isEmpty()) {
        try {
          return AggregationType.valueOf(trimmed.toUpperCase(Locale.ENGLISH));
        } catch (final IllegalArgumentException e) {
          throw unknownAggregationType(path, rawType, e);
        }
      }
    }
    throw unknownAggregationType(path, rawType, null);
  }

  /**
   * Refusal of an aggregation function name, worded identically on both time-series HTTP endpoints so the two
   * surfaces report the same thing. Names the field, lists every accepted value, and echoes what arrived.
   */
  private static IllegalArgumentException unknownAggregationType(final String path, final Object rawType,
      final Throwable cause) {
    final StringJoiner accepted = new StringJoiner(", ");
    for (final AggregationType type : AggregationType.values())
      accepted.add(type.name());

    return new IllegalArgumentException(
        "'" + path + "' is required and must be one of " + accepted
            + (rawType == null ? "" : ": received '" + truncate(String.valueOf(rawType)) + "'"), cause);
  }

  private static String truncate(final String value) {
    return value.length() <= MAX_ECHOED_VALUE_LENGTH ? value
        : value.substring(0, MAX_ECHOED_VALUE_LENGTH) + "...";
  }

  /**
   * Builds the conjunction of a {@code tags} request object: every name/value pair becomes an ANDed condition.
   *
   * @throws IllegalArgumentException if a name resolves to no TAG column of the type, or a value cannot be
   *                                  stored as a tag. See {@link TimeSeriesGateway#andTag}
   */
  static TagFilter buildTagFilter(final JSONObject tagsJson, final List<ColumnDefinition> columns) {
    if (tagsJson == null || tagsJson.keySet().isEmpty())
      return null;

    TagFilter filter = null;
    for (final String tagName : tagsJson.keySet())
      filter = TimeSeriesGateway.andTag(filter, tagName, tagsJson.get(tagName), columns);

    return filter;
  }

  /**
   * Builds the conjunction of a repeated {@code name:value} query parameter, the form
   * {@code GET /ts/{database}/latest} takes its tag filter in. Every occurrence that resolves to a TAG
   * column becomes an ANDed condition, so {@code ?tag=host:a&tag=region:eu} names one series rather than
   * narrowing on {@code host} alone (issue #7321).
   * <p>
   * The name is separated from the value by the FIRST {@code ':'}, which leaves any further colon to the
   * value - a tag value is caller text and may well contain one.
   * <p>
   * Occurrences are ANDed, including two that name the SAME tag: {@code ?tag=host:a&tag=host:b} asks for a
   * sample whose host is both, which no sample is, so it selects nothing. That is the literal reading of
   * "every occurrence must match" the OpenAPI description gives, and it is deliberate rather than an
   * accident of the loop - a set-membership (IN) reading would need {@link TagFilter#andIn} and its own
   * documented syntax.
   * <p>
   * An occurrence that is not in {@code name:value} form is REFUSED rather than skipped (issue #7334), and so
   * is a name that resolves to no TAG column - the latter inside {@link TimeSeriesGateway#andTag}, which every
   * protocol's tag selection shares. Skipping either dropped one term of the conjunction, which on this
   * endpoint means {@code ?tag=hsot:web1} answered the newest sample of ANY series: a widened query the caller
   * cannot tell apart from a correct filter that matched everything. A blank occurrence is still ignored,
   * because a bare {@code ?tag=} or {@code &tag=&} is what an empty form field produces and says nothing.
   *
   * @param tagParams every occurrence of the parameter, in the order the request carried them; may be
   *                  {@code null} or empty, which yields no filter
   *
   * @throws IllegalArgumentException if an occurrence carries no {@code ':'} separator, or names no TAG column
   */
  static TagFilter buildTagFilterFromQueryParams(final Collection<String> tagParams,
      final List<ColumnDefinition> columns) {
    if (tagParams == null || tagParams.isEmpty())
      return null;

    TagFilter filter = null;
    for (final String tagParam : tagParams) {
      if (tagParam == null || tagParam.isBlank())
        continue;

      final int colonIdx = tagParam.indexOf(':');
      if (colonIdx <= 0)
        throw new IllegalArgumentException("The 'tag' parameter must be in 'name:value' form: received '"
            + truncate(tagParam) + "'");

      filter = TimeSeriesGateway.andTag(filter, tagParam.substring(0, colonIdx),
          tagParam.substring(colonIdx + 1), columns);
    }

    return filter;
  }

  /**
   * Resolves a {@code fields} projection to the column indices the engine takes.
   * <p>
   * Only the JSON SHAPE of the projection is checked here. A well-formed name that matches no column is DROPPED
   * by {@link TimeSeriesGateway#resolveColumnIndices}, not refused - see its Javadoc, and
   * {@code TimeSeriesGatewayProjectionTest}, which pins that. So {@code "fields": ["temprature"]} still answers
   * 200 with a timestamp-only row rather than naming the typo, which is the same widening #7334 refused for a tag
   * name and is tracked separately.
   *
   * @param path the projection's request path, e.g. {@code fields} or {@code targets[0].fields}, used to name an
   *             element that is not a string (issue #7340)
   *
   * @throws IllegalArgumentException if an element is absent, null or not a string
   */
  static int[] resolveColumnIndices(final JSONArray fieldsJson, final List<ColumnDefinition> columns,
      final String path) {
    if (fieldsJson == null || fieldsJson.length() == 0)
      return null;

    final List<String> fields = new ArrayList<>(fieldsJson.length());
    for (int f = 0; f < fieldsJson.length(); f++)
      fields.add(requireStringElement(fieldsJson, f, path + "[" + f + "]"));

    return TimeSeriesGateway.resolveColumnIndices(fields, columns);
  }

  static int findColumnIndex(final String fieldName, final List<ColumnDefinition> columns) {
    return TimeSeriesGateway.findColumnIndex(fieldName, columns);
  }

  /**
   * Renders a refused request as the 400 the TimeSeries read endpoints answer with, carrying the reason in
   * {@code error} (issues #7334, #7340).
   * <p>
   * Answered explicitly rather than by letting the exception reach the generic handler mapper, which does map an
   * {@link IllegalArgumentException} to a 400 but puts its text in {@code detail} - a field {@code buildErrorBody}
   * CONCEALS outside development mode. The whole point of these refusals is that the caller reads which tag did
   * not resolve, or which member was missing, so the message has to be in the field that is always sent.
   * <p>
   * Built with {@link JSONObject} rather than string concatenation because the message echoes caller text, which
   * can carry a double quote or a backslash that raw concatenation would turn into invalid JSON.
   */
  static ExecutionResponse badRequest(final IllegalArgumentException e) {
    return new ExecutionResponse(400, new JSONObject().put("error", e.getMessage()).toString());
  }

  /**
   * Renders a failed {@link TimeSeriesGateway#resolveForRead} as the 400 the TimeSeries read endpoints answer
   * with. The three cases stay distinct: a type that IS a TimeSeries type whose storage failed to load used to
   * share the "is not a TimeSeries type" message, which sent an operator chasing the wrong cause (issue #6356
   * follow-up, claude-review on PR #6779).
   * <p>
   * The engine-unavailable body is built with {@link JSONObject} rather than string concatenation because the
   * reason embeds a file path that could contain a double quote or a backslash, which raw concatenation would
   * turn into invalid JSON.
   */
  static ExecutionResponse resolutionError(final String typeName, final TypeResolution resolved) {
    return switch (resolved.failure()) {
      case NOT_FOUND -> new ExecutionResponse(400,
          "{ \"error\" : \"Type '" + typeName + "' does not exist\"}");
      case NOT_TIME_SERIES -> new ExecutionResponse(400,
          "{ \"error\" : \"Type '" + typeName + "' is not a TimeSeries type\"}");
      case ENGINE_UNAVAILABLE -> new ExecutionResponse(400, new JSONObject().put("error",
          "TimeSeries type '" + typeName + "' has no storage engine available: " + resolved.unavailableReason())
          .toString());
    };
  }
}
