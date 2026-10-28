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
package com.arcadedb.remote;

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.Database;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.facet.AdvancedFacetsFixture;
import com.arcadedb.query.facet.FacetBucket;
import com.arcadedb.query.facet.FacetResult;
import com.arcadedb.query.facet.FacetResults;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.FacetsHttpIT;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** {@link RemoteDatabase} hands back the facets of a FACETS query on its buffered and streamed paths, parsed with FacetResults.fromJSON. */
public class RemoteDatabaseFacetsIT extends BaseGraphServerTest {
  private static final String TYPE  = ColumnarTestData.TYPE;
  private static final String QUERY = "SELECT FROM " + TYPE + " WHERE fileSize > 0 FACETS brand TERMS TOP 5, tags TERMS TOP 3 LIMIT 4";

  @Override
  protected int getServerCount() {
    return 1;
  }

  @Override
  protected void populateDatabase() {
    super.populateDatabase();
    // assets tagged from a tag tree (plan 5 HIERARCHY) and the measures of the plan 5 STATS types
    AdvancedFacetsFixture.load(getDatabase(0), 11L, 1_000);
  }

  /** The server reopened the database: its projections are awaited again before a test compares facets. */
  @BeforeEach
  void awaitProjections() {
    FacetOracle.awaitCaughtUp(getServerDatabase(0, getDatabaseName()));
  }

  private RemoteDatabase remote() {
    return new RemoteDatabase("127.0.0.1", getServerHttpPort(0), getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);
  }

  @Test
  void remoteQueryReturnsFacets() {
    try (final RemoteDatabase remote = remote(); final ResultSet rs = remote.query("sql", QUERY)) {
      assertThat(rs.stream().count()).isEqualTo(4);
      assertSameFacets(rs.getFacets());
    }
  }

  @Test
  void remoteCommandReturnsFacets() {
    try (final RemoteDatabase remote = remote(); final ResultSet rs = remote.command("sql", QUERY)) {
      assertSameFacets(rs.getFacets());
      assertThat(rs.stream().count()).isEqualTo(4);
    }
  }

  @Test
  void remoteStreamingReturnsFacetsAfterRows() {
    try (final RemoteDatabase remote = remote(); final ResultSet rs = remote.queryStream("sql", QUERY)) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.getFacets()).isNull();
      int rows = 0;
      while (rs.hasNext()) {
        rs.next();
        ++rows;
      }
      assertThat(rows).isEqualTo(4);
      assertSameFacets(rs.getFacets());
    }
  }

  @Test
  void noFacetsWithoutClause() {
    try (final RemoteDatabase remote = remote()) {
      try (final ResultSet rs = remote.query("sql", "SELECT FROM " + TYPE + " LIMIT 2")) {
        assertThat(rs.stream().count()).isEqualTo(2);
        assertThat(rs.getFacets()).isNull();
      }
      try (final ResultSet rs = remote.queryStream("sql", "SELECT FROM " + TYPE + " LIMIT 2")) {
        assertThat(rs.stream().count()).isEqualTo(2);
        assertThat(rs.getFacets()).isNull();
      }
    }
  }

  /**
   * Final review C-I8: the Java client decodes a facet's values as it decodes a row's value of the same property, on the buffered and
   * the streamed path: a DATE, a DATETIME, a DECIMAL and a NaN term are objects equal to (and of the class of) the row's.
   */
  @Test
  void facetValuesDecodeAsRowValues() {
    FacetsHttpIT.createTypedValues(getServerDatabase(0, getDatabaseName()));
    final String facets = "SELECT FROM Typed FACETS d: day TERMS, a: at TERMS, p: price TERMS, s: score TERMS";
    try (final RemoteDatabase remote = remote()) {
      final Map<String, Set<Object>> rows = new HashMap<>();
      try (final ResultSet rs = remote.query("sql", "SELECT day AS d, at AS a, price AS p, score AS s FROM Typed")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          for (final String alias : List.of("d", "a", "p", "s"))
            rows.computeIfAbsent(alias, k -> new HashSet<>()).add(row.getProperty(alias));
        }
      }
      final FacetResults buffered;
      try (final ResultSet rs = remote.query("sql", facets)) {
        buffered = rs.getFacets();
      }
      final FacetResults streamed;
      try (final ResultSet rs = remote.queryStream("sql", facets)) {
        rs.stream().count();
        streamed = rs.getFacets();
      }
      for (final String alias : List.of("d", "a", "p", "s")) {
        assertThat(termValues(buffered.get(alias))).as("buffered " + alias).isEqualTo(rows.get(alias));
        assertThat(termValues(streamed.get(alias))).as("streamed " + alias).isEqualTo(rows.get(alias));
      }
      // the rows' own types, restored from the type hints: a date, not its text
      assertThat(rows.get("d").iterator().next()).isNotInstanceOf(String.class);
      assertThat(rows.get("p").iterator().next()).isInstanceOf(BigDecimal.class);
    }
  }

  /**
   * Plan 5 Task 14: HIERARCHY attributes, DISTINCT, STATS (LONG, DATETIME, FLOAT, SHORT, INTEGER) and the typed cursor reach the Java
   * client with the values and Java types the embedded API gives, on the buffered, command and streamed paths; and a STATS facet
   * equals the {@code min()}, {@code max()}, {@code sum()} and {@code avg()} the same client reads from rows, value and class.
   */
  @Test
  void plan5KindsDecodeAsTheEmbeddedApi() {
    final Database server = getServerDatabase(0, getDatabaseName());
    try (final RemoteDatabase remote = remote()) {
      for (final String query : List.of(AdvancedFacetsFixture.ASSET_QUERY, AdvancedFacetsFixture.MEASURE_QUERY)) {
        final FacetResults embedded = AdvancedFacetsFixture.facets(server, query);
        try (final ResultSet rs = remote.query("sql", query)) {
          AdvancedFacetsFixture.assertSameValues("buffered", rs.getFacets(), embedded);
        }
        try (final ResultSet rs = remote.command("sql", query)) {
          AdvancedFacetsFixture.assertSameValues("command", rs.getFacets(), embedded);
        }
        try (final ResultSet rs = remote.queryStream("sql", query)) {
          rs.stream().count();
          AdvancedFacetsFixture.assertSameValues("streamed", rs.getFacets(), embedded);
        }
      }
      AdvancedFacetsFixture.assertStatsEqualRows("remote", AdvancedFacetsFixture.facets(remote, AdvancedFacetsFixture.MEASURE_QUERY),
          AdvancedFacetsFixture.measureAggregates(remote));
      AdvancedFacetsFixture.assertStatsEqualRows("embedded", AdvancedFacetsFixture.facets(server, AdvancedFacetsFixture.MEASURE_QUERY),
          AdvancedFacetsFixture.measureAggregates(server));
    }
  }

  /** The typed cursor a remote page returns, sent back as AFTER, starts the next page where the embedded paging starts it. */
  @Test
  void typedCursorPagesAsEmbedded() {
    final Database server = getServerDatabase(0, getDatabaseName());
    final String first = "SELECT FROM " + TYPE + " FACETS pd: publishDate TERMS TOP 4 ORDER BY VALUE LIMIT 1";
    final String next = "SELECT FROM " + TYPE + " FACETS pd: publishDate TERMS TOP 4 AFTER :c LIMIT 1";
    try (final RemoteDatabase remote = remote()) {
      FacetResult remotePage = AdvancedFacetsFixture.facets(remote, first).get("pd");
      FacetResult embeddedPage = AdvancedFacetsFixture.facets(server, first).get("pd");
      for (int page = 0; page < 3; page++) {
        assertThat(remotePage.after()).as("page %d", page).isNotNull();
        assertThat(remotePage.after()).isEqualTo(embeddedPage.after());
        assertThat(remotePage.after().getClass()).isEqualTo(embeddedPage.after().getClass());
        final Object remoteCursor = remotePage.after();
        final Object embeddedCursor = embeddedPage.after();
        try (final ResultSet rs = remote.query("sql", next, Map.of("c", remoteCursor))) {
          remotePage = rs.getFacets().get("pd");
        }
        try (final ResultSet rs = server.query("sql", next, Map.of("c", embeddedCursor))) {
          embeddedPage = rs.getFacets().get("pd");
        }
        assertThat(termList(remotePage)).as("page %d", page + 1).isEqualTo(termList(embeddedPage));
      }
    }
  }

  /** {@code facet.searchValues} rows read by the Java client: value and count of the embedded rows, the count a Long. */
  @Test
  void searchValuesRowsAsEmbedded() {
    final String sql = "SELECT expand(facet.searchValues('Asset', 'tags', 't/2', 6))";
    try (final RemoteDatabase remote = remote()) {
      final List<Map<String, Object>> remoteRows = rows(remote, sql);
      assertThat(remoteRows).hasSize(6);
      assertThat(remoteRows).isEqualTo(rows(getServerDatabase(0, getDatabaseName()), sql));
      assertThat(remoteRows.getFirst().get("count")).isInstanceOf(Long.class);
    }
  }

  private static List<Map<String, Object>> rows(final BasicDatabase db, final String sql) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    try (final ResultSet rs = db.query("sql", sql)) {
      while (rs.hasNext())
        rows.add(rs.next().toMap());
    }
    return rows;
  }

  private static List<Object> termList(final FacetResult result) {
    final List<Object> values = new ArrayList<>();
    for (final FacetBucket bucket : result.buckets())
      values.add(bucket.isMissing() ? "<missing>" : bucket.value() + "=" + bucket.count());
    return values;
  }

  private static Set<Object> termValues(final FacetResult result) {
    final Set<Object> values = new HashSet<>();
    for (final FacetBucket bucket : result.buckets())
      if (!bucket.isMissing())
        values.add(bucket.value());
    return values;
  }

  private void assertSameFacets(final FacetResults remoteFacets) {
    assertThat(remoteFacets).isNotNull();
    final JSONObject expected;
    try (final ResultSet rs = getServerDatabase(0, getDatabaseName()).query("sql", QUERY)) {
      expected = rs.getFacets().toJSON();
    }
    assertThat(remoteFacets.toJSON().toString()).isEqualTo(expected.toString());
  }
}
