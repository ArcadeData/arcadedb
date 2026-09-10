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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.StringJoiner;

/**
 * Shared utilities for TimeSeries HTTP handlers.
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
    final Object rawType = request.opt("type");
    if (rawType instanceof String name) {
      final String trimmed = name.trim();
      if (!trimmed.isEmpty()) {
        try {
          return AggregationType.valueOf(trimmed.toUpperCase(Locale.ENGLISH));
        } catch (final IllegalArgumentException e) {
          throw unknownAggregationType(index, rawType, e);
        }
      }
    }
    throw unknownAggregationType(index, rawType, null);
  }

  /**
   * Refusal of an aggregation function name, worded identically on both time-series HTTP endpoints so the two
   * surfaces report the same thing. Names the field, lists every accepted value, and echoes what arrived.
   */
  private static IllegalArgumentException unknownAggregationType(final int index, final Object rawType,
      final Throwable cause) {
    final StringJoiner accepted = new StringJoiner(", ");
    for (final AggregationType type : AggregationType.values())
      accepted.add(type.name());

    return new IllegalArgumentException(
        "'aggregation.requests[" + index + "].type' is required and must be one of " + accepted
            + (rawType == null ? "" : ": received '" + truncate(String.valueOf(rawType)) + "'"), cause);
  }

  private static String truncate(final String value) {
    return value.length() <= MAX_ECHOED_VALUE_LENGTH ? value
        : value.substring(0, MAX_ECHOED_VALUE_LENGTH) + "...";
  }

  /**
   * Builds the conjunction of a {@code tags} request object: every name/value pair that resolves to a TAG
   * column becomes an ANDed condition.
   */
  static TagFilter buildTagFilter(final JSONObject tagsJson, final List<ColumnDefinition> columns) {
    if (tagsJson == null || tagsJson.keySet().isEmpty())
      return null;

    TagFilter filter = null;
    for (final String tagName : tagsJson.keySet())
      filter = andTag(filter, tagName, tagsJson.get(tagName), columns);

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
   *
   * @param tagParams every occurrence of the parameter, in the order the request carried them; may be
   *                  {@code null} or empty, which yields no filter
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
        // Not in name:value form, so it names no column. Silently skipped, as an unresolvable name is on
        // the 'tags' object path above - reporting either is issue #7334.
        continue;

      filter = andTag(filter, tagParam.substring(0, colonIdx), tagParam.substring(colonIdx + 1), columns);
    }

    return filter;
  }

  /**
   * ANDs one tag condition onto {@code filter}, resolving {@code tagName} to its position among the
   * non-timestamp columns. Shared by both builders above so the two endpoints cannot drift apart on how a
   * tag is resolved, which is how they came to disagree in the first place (issue #7321).
   *
   * @return {@code filter} unchanged when no TAG column carries that name, a new filter otherwise -
   * {@link TagFilter} is immutable, so the return value must be used
   */
  private static TagFilter andTag(final TagFilter filter, final String tagName, final Object tagValue,
      final List<ColumnDefinition> columns) {
    int nonTsIdx = 0;
    for (final ColumnDefinition col : columns) {
      if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
        continue;
      if (col.getRole() == ColumnDefinition.ColumnRole.TAG && col.getName().equals(tagName)) {
        // Coerce the value to the column's declared type so it matches what both storage layers hand
        // back (issue #5475).
        final Object coerced = col.coerceValue(tagValue);
        return filter == null ? TagFilter.eq(nonTsIdx, coerced) : filter.and(nonTsIdx, coerced);
      }
      nonTsIdx++;
    }
    return filter;
  }

  static int[] resolveColumnIndices(final JSONArray fieldsJson, final List<ColumnDefinition> columns) {
    if (fieldsJson == null || fieldsJson.length() == 0)
      return null;

    final List<Integer> indices = new ArrayList<>();

    // Always include timestamp
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).getRole() == ColumnDefinition.ColumnRole.TIMESTAMP) {
        indices.add(i);
        break;
      }
    }

    for (int f = 0; f < fieldsJson.length(); f++) {
      final String fieldName = fieldsJson.getString(f);
      for (int i = 0; i < columns.size(); i++) {
        if (columns.get(i).getName().equals(fieldName) &&
            columns.get(i).getRole() != ColumnDefinition.ColumnRole.TIMESTAMP) {
          indices.add(i);
          break;
        }
      }
    }

    return indices.stream().mapToInt(Integer::intValue).toArray();
  }

  static int findColumnIndex(final String fieldName, final List<ColumnDefinition> columns) {
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).getName().equals(fieldName))
        return i;
    }
    return -1;
  }
}
