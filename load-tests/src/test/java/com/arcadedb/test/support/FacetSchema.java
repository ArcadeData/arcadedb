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
package com.arcadedb.test.support;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.io.IOException;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * The {@code FacetAsset} schema, every statement the facet tests send, the {@code FACETS} query and the equivalent {@code GROUP BY}
 * queries, normalized into the key format of {@link FacetSnapshot}. Every truth query reads from a subquery or carries a WHERE, so
 * the {@code GROUP BY} acceleration (which takes only a whereless {@code GROUP BY} over a schema type) never answers it: the truth
 * reads the records, not the projection.
 */
public final class FacetSchema {
  public static final String TYPE = "FacetAsset";

  public static final List<String> DDL = List.of(//
      "CREATE VERTEX TYPE FacetAsset IF NOT EXISTS", //
      "CREATE EDGE TYPE FacetLink IF NOT EXISTS", //
      "CREATE PROPERTY FacetAsset.aid IF NOT EXISTS LONG", //
      "CREATE PROPERTY FacetAsset.brand IF NOT EXISTS STRING", //
      "CREATE PROPERTY FacetAsset.tags IF NOT EXISTS LIST OF STRING", //
      "CREATE PROPERTY FacetAsset.fileSize IF NOT EXISTS LONG", //
      "CREATE INDEX IF NOT EXISTS ON FacetAsset (aid) UNIQUE", //
      "CREATE INDEX IF NOT EXISTS ON FacetAsset (brand) COLUMNAR", //
      "CREATE INDEX IF NOT EXISTS ON FacetAsset (tags BY ITEM) COLUMNAR", //
      "CREATE INDEX IF NOT EXISTS ON FacetAsset (fileSize) COLUMNAR");

  /** The filters compared by the checks: no WHERE, and one that excludes the smallest range. */
  public static final List<String> CHECKED_FILTERS = List.of("", "fileSize >= 1024");

  public static final long SMALL_MAX  = 1024L;
  public static final long MEDIUM_MAX = 1_048_576L;

  public static final String INSERT          = "INSERT INTO FacetAsset SET aid = :aid, brand = :brand, tags = :tags, fileSize = :fileSize, body = :body";
  public static final String INSERT_NO_BRAND = "INSERT INTO FacetAsset SET aid = :aid, tags = :tags, fileSize = :fileSize, body = :body";
  public static final String UPDATE_VALUES   = "UPDATE FacetAsset SET brand = :brand, tags = :tags, fileSize = :fileSize WHERE aid = :aid";
  public static final String REMOVE_BRAND    = "UPDATE FacetAsset REMOVE brand WHERE aid = :aid";
  public static final String GROW            = "UPDATE FacetAsset SET body = :body, brand = :brand WHERE aid = :aid";
  public static final String UPDATE_GROWN    = "UPDATE FacetAsset SET brand = :brand, fileSize = :fileSize WHERE aid = :aid";
  public static final String DELETE          = "DELETE FROM FacetAsset WHERE aid = :aid";
  public static final String LINK            =
      "CREATE EDGE FacetLink FROM (SELECT FROM FacetAsset WHERE aid = :from) TO (SELECT FROM FacetAsset WHERE aid = :to)";

  private static final Pattern WHERE_PATTERN = Pattern.compile("[A-Za-z0-9_ >=<.']*");

  /** Runs a query and returns its rows as JSON. */
  @FunctionalInterface
  public interface Rows {
    JSONArray query(String sql) throws IOException;
  }

  private FacetSchema() {
  }

  /** @return the {@code FACETS} query over the assets matching {@code where} (empty for all) */
  public static String facetQuery(final String where) {
    return "SELECT @rid FROM FacetAsset" + whereClause(where) + " FACETS brand TERMS TOP 1000, tags TERMS TOP 1000, "
        + "size: fileSize RANGES ([0, 1024) AS 'small', [1024, 1048576) AS 'medium', [1048576, *) AS 'large') LIMIT 1";
  }

  /**
   * @return the counts of the same facets computed with {@code GROUP BY} over the records, in the key format of {@link FacetSnapshot}
   */
  public static SortedMap<String, Long> expectedCounts(final Rows rows, final String where) throws IOException {
    final String filter = whereClause(where);
    final String and = where.isEmpty() ? "" : where + " AND ";
    final SortedMap<String, Long> counts = new TreeMap<>();

    final JSONArray brands = rows.query("SELECT v, count(*) AS c FROM (SELECT brand AS v FROM FacetAsset" + filter + ") GROUP BY v");
    for (int i = 0; i < brands.length(); i++) {
      final JSONObject row = brands.getJSONObject(i);
      add(counts, row.isNull("v") ? "brand.missing" : "brand=" + row.getString("v"), row.getLong("c", 0L));
    }

    final JSONArray tags = rows.query(
        "SELECT tag AS v, count(*) AS c FROM (SELECT tags AS tag FROM FacetAsset" + filter + " UNWIND tag) GROUP BY tag");
    for (int i = 0; i < tags.length(); i++) {
      final JSONObject row = tags.getJSONObject(i);
      add(counts, row.isNull("v") ? "tags.missing" : "tags=" + row.getString("v"), row.getLong("c", 0L));
    }

    add(counts, "size=small", count(rows, and + "fileSize >= 0 AND fileSize < " + SMALL_MAX));
    add(counts, "size=medium", count(rows, and + "fileSize >= " + SMALL_MAX + " AND fileSize < " + MEDIUM_MAX));
    add(counts, "size=large", count(rows, and + "fileSize >= " + MEDIUM_MAX));
    add(counts, "size.missing", count(rows, and + "fileSize IS NULL"));
    return counts;
  }

  private static long count(final Rows rows, final String where) throws IOException {
    return rows.query("SELECT count(*) AS c FROM FacetAsset WHERE " + where).getJSONObject(0).getLong("c", 0L);
  }

  private static void add(final SortedMap<String, Long> counts, final String key, final long count) {
    if (count != 0L)
      counts.merge(key, count, Long::sum);
  }

  private static String whereClause(final String where) {
    if (!WHERE_PATTERN.matcher(where).matches())
      throw new IllegalArgumentException("Not a plain filter: " + where);
    return where.isEmpty() ? "" : " WHERE " + where;
  }
}
