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
import java.util.List;
import java.util.Locale;
import java.util.StringJoiner;

/**
 * Shared utilities for TimeSeries HTTP handlers.
 */
final class TimeSeriesHandlerUtils {

  private TimeSeriesHandlerUtils() {
  }

  /**
   * Longest caller-supplied value echoed back in the refusal below. The payload is already bounded by the HTTP
   * body-size limit, so this only keeps a long-but-legal string out of an error body that exists to be read.
   */
  private static final int MAX_ECHOED_VALUE_LENGTH = 64;

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

  static TagFilter buildTagFilter(final JSONObject tagsJson, final List<ColumnDefinition> columns) {
    if (tagsJson == null || tagsJson.keySet().isEmpty())
      return null;

    TagFilter filter = null;

    for (final String tagName : tagsJson.keySet()) {
      final Object tagValue = tagsJson.get(tagName);

      int nonTsIdx = 0;
      for (final ColumnDefinition col : columns) {
        if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
          continue;
        if (col.getRole() == ColumnDefinition.ColumnRole.TAG && col.getName().equals(tagName)) {
          // Coerce the JSON value to the column's declared type so it matches what both storage layers
          // hand back (issue #5475).
          final Object coerced = col.coerceValue(tagValue);
          if (filter == null)
            filter = TagFilter.eq(nonTsIdx, coerced);
          else
            filter = filter.and(nonTsIdx, coerced);
          break;
        }
        nonTsIdx++;
      }
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
