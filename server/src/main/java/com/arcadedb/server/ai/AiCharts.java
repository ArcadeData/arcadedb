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
package com.arcadedb.server.ai;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The charts the AI Assistant asks Studio to draw. The portal validates them already; the server checks them again before they
 * reach the browser or the saved chat, because a chart carries a QUERY that Studio will run. Only well-formed charts survive:
 * one of the known types, a query language Studio can run, a query of a sane size, and column names (not expressions).
 *
 * <p>The model never sees the rows: Studio runs the query itself, read-only, and draws it.
 */
final class AiCharts {
  static final int MAX_CHARTS      = 3;
  static final int MAX_QUERY       = 4_000;
  static final int MAX_TITLE       = 120;
  static final int MAX_SERIES      = 5;

  private static final Set<String> TYPES     = Set.of("bar", "horizontalBar", "line", "area", "pie", "donut");
  private static final Set<String> LANGUAGES = Set.of("sql", "cypher", "gremlin", "graphql");
  private static final Pattern     COLUMN    = Pattern.compile("^[A-Za-z_@][A-Za-z0-9_.@$-]{0,63}$");

  private AiCharts() {
  }

  /** The valid charts of {@code raw}, at most {@link #MAX_CHARTS}; never null. */
  static JSONArray clean(final JSONArray raw) {
    final JSONArray out = new JSONArray();
    if (raw == null)
      return out;
    for (int i = 0; i < raw.length() && out.length() < MAX_CHARTS; i++) {
      final JSONObject chart = raw.get(i) instanceof JSONObject o ? o : null;
      final JSONObject clean = chart == null ? null : cleanOne(chart);
      if (clean != null)
        out.put(clean);
    }
    return out;
  }

  private static JSONObject cleanOne(final JSONObject chart) {
    final String type = chart.getString("type", "");
    if (!TYPES.contains(type))
      return null;
    final String language = chart.getString("language", "sql").toLowerCase(Locale.ROOT);
    if (!LANGUAGES.contains(language))
      return null;
    final String query = chart.getString("query", "").trim();
    if (query.isEmpty() || query.length() > MAX_QUERY)
      return null;
    final String x = chart.getString("x", "");
    if (!COLUMN.matcher(x).matches())
      return null;
    final JSONArray rawY = chart.getJSONArray("y", null);
    if (rawY == null || rawY.length() == 0 || rawY.length() > MAX_SERIES)
      return null;
    final JSONArray y = new JSONArray();
    for (int i = 0; i < rawY.length(); i++) {
      final Object column = rawY.get(i);
      if (!(column instanceof String name) || !COLUMN.matcher(name).matches())
        return null;
      y.put(name);
    }
    String title = chart.getString("title", "");
    if (title.length() > MAX_TITLE)
      title = title.substring(0, MAX_TITLE);
    return new JSONObject().put("type", type).put("title", title).put("language", language).put("query", query).put("x", x)
        .put("y", y);
  }
}
