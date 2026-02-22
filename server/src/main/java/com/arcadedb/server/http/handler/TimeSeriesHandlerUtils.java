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

import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.TagFilter;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Shared utilities for TimeSeries HTTP handlers.
 */
final class TimeSeriesHandlerUtils {

  private TimeSeriesHandlerUtils() {
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
          if (filter == null)
            filter = TagFilter.eq(nonTsIdx, tagValue);
          else
            filter = filter.and(nonTsIdx, tagValue);
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
