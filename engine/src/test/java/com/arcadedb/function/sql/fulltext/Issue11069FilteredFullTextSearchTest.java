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
package com.arcadedb.function.sql.fulltext;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11069: a full-text search combined with a filter on another property. The SQL planner gave up the limit for any
 * conjunct besides the search and loaded matches until enough passed the filter, nearly every match for a restrictive tenant;
 * {@code db.index.fulltext.queryNodes} had no way to filter before its own {@code limit}, so a {@code WHERE} after the
 * {@code CALL} answered too few rows.
 * <p>
 * Corpus: {@value #DOCUMENTS} vertices that all match {@code common}. Tenant {@code a} owns most of them and scores highest;
 * tenant {@code b} owns {@value #TENANT_B_DOCUMENTS} and scores lowest, the worst case for a filter that rejects the best
 * matches first.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11069FilteredFullTextSearchTest extends TestHelper {
  private static final int    DOCUMENTS          = 2_000;
  private static final int    TENANT_B_DOCUMENTS = 20;
  private static final int    WANTED             = 10;
  /** Loads tolerated beyond WANTED: none are needed when the filter is decided before the load. */
  private static final int    SLACK              = 5;
  private static final String SEARCH             = "SEARCH_INDEX('Doc[content]', 'common') = true";

  @Test
  void sqlRestrictiveFilterIsDecidedBeforeTheLoad() {
    createCorpus(1, true);
    final String query = "SELECT FROM Doc WHERE " + SEARCH + " AND tenant = 'b' LIMIT " + WANTED;

    assertThat(plan(query)).contains("(best " + WANTED + " needed)").contains("INDEX PRE-FILTER (tenant = 'b')");
    assertThat(recordsLoadedBy(query)).as("records loaded for LIMIT " + WANTED + " of " + DOCUMENTS + " matches")
        .isLessThanOrEqualTo(WANTED + SLACK);
    assertThat(tenantsOf(query)).containsOnly("b").hasSize(WANTED);
  }

  @Test
  void sqlRestrictiveFilterOverSeveralBuckets() {
    createCorpus(4, true);
    final String query = "SELECT @rid AS rid, $score AS score FROM Doc WHERE " + SEARCH + " AND tenant = :t ORDER BY $score DESC LIMIT " + WANTED;

    assertThat(recordsLoadedBy(query, Map.of("t", "b"))).isLessThanOrEqualTo(WANTED + SLACK);
    assertSameAnswerAsTheUnfilteredSearch(query, Map.of("t", "b"));
  }

  @Test
  void sqlPermissiveFilterStaysCheap() {
    createCorpus(1, true);
    final String query = "SELECT FROM Doc WHERE " + SEARCH + " AND tenant = 'a' LIMIT " + WANTED;
    assertThat(recordsLoadedBy(query)).isLessThanOrEqualTo(WANTED + SLACK);
    assertThat(tenantsOf(query)).containsOnly("a").hasSize(WANTED);
  }

  @Test
  void sqlFilterWithoutAnIndexStaysCorrect() {
    createCorpus(4, false);
    final String query = "SELECT @rid AS rid, $score AS score FROM Doc WHERE " + SEARCH + " AND tenant = :t ORDER BY $score DESC LIMIT " + WANTED;
    // no index lists the tenant: the records are tested once loaded, and the limit is not cut before the test
    assertSameAnswerAsTheUnfilteredSearch(query, Map.of("t", "b"));
    assertThat(tenantsOf("SELECT FROM Doc WHERE " + SEARCH + " AND tenant = 'b' SKIP 15 LIMIT " + WANTED)).containsOnly("b")
        .hasSize(TENANT_B_DOCUMENTS - 15);
  }

  @Test
  void sqlCaseInsensitiveIndexListsASuperset() {
    createCorpus(2, false);
    database.command("sql", "CREATE INDEX ON Doc (tenant COLLATE ci) NOTUNIQUE");
    // 'B' matches no record with the case-sensitive '=', although the case-insensitive index lists every 'b'
    assertThat(tenantsOf("SELECT FROM Doc WHERE " + SEARCH + " AND tenant = 'B' LIMIT " + WANTED)).isEmpty();
    assertSameAnswerAsTheUnfilteredSearch("SELECT @rid AS rid, $score AS score FROM Doc WHERE " + SEARCH + " AND tenant = :t ORDER BY $score DESC LIMIT "
        + WANTED, Map.of("t", "b"));
  }

  @Test
  void sqlFilterWithAnotherConjunctAndARidCondition() {
    createCorpus(1, true);
    final List<RID> tenantB = new ArrayList<>(matchesByScore("b").keySet());
    final RID cursor = tenantB.stream().sorted().toList().get(4);
    final String query = "SELECT FROM Doc WHERE " + SEARCH + " AND tenant = 'b' AND @rid > :cursor AND k >= 0 LIMIT " + WANTED;
    final List<RID> expected = new ArrayList<>();
    for (final RID rid : tenantB)
      if (rid.compareTo(cursor) > 0 && expected.size() < WANTED)
        expected.add(rid);
    final List<RID> found = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query, Map.of("cursor", cursor))) {
      while (rs.hasNext())
        found.add(rs.next().getIdentity().orElseThrow());
    }
    assertThat(found).containsExactlyInAnyOrderElementsOf(expected);
  }

  @Test
  void cypherWhereOptionFiltersBeforeTheLimit() {
    createCorpus(2, true);
    // The WHERE after the CALL only sees the procedure's own best rows, as in Neo4j: none of them is of tenant b
    assertThat(cypherTenants("CALL db.index.fulltext.queryNodes('Doc[content]', 'common', {limit: " + WANTED + "}) YIELD node "
        + "WHERE node.tenant = 'b' RETURN node", Map.of())).isEmpty();

    final long before = readRecords();
    assertThat(cypherTenants("CALL db.index.fulltext.queryNodes('Doc[content]', 'common', {limit: " + WANTED
        + ", where: {tenant: $t}}) YIELD node RETURN node", Map.of("t", "b"))).containsOnly("b").hasSize(WANTED);
    assertThat(readRecords() - before).isLessThanOrEqualTo(2L * (WANTED + SLACK));
  }

  @Test
  void cypherWhereOptionWithoutAnIndexAndAsACondition() {
    createCorpus(2, false);
    assertThat(cypherTenants("CALL db.index.fulltext.queryNodes('Doc[content]', 'common', {limit: " + WANTED
        + ", where: {tenant: 'b'}}) YIELD node RETURN node", Map.of())).containsOnly("b").hasSize(WANTED);
    assertThat(cypherTenants("CALL db.index.fulltext.queryNodes('Doc[content]', 'common', {skip: 15, limit: " + WANTED
        + ", where: 'tenant = :t'}) YIELD node RETURN node", Map.of("t", "b"))).containsOnly("b").hasSize(TENANT_B_DOCUMENTS - 15);

    // the scores of the filtered rows are those of the unfiltered search
    final Map<RID, Float> unfiltered = matchesByScore(null);
    try (final ResultSet rs = database.query("opencypher", "CALL db.index.fulltext.queryNodes('Doc[content]', 'common', {limit: 3, "
        + "where: {tenant: 'b'}}) YIELD node, score RETURN node, score")) {
      final Map<RID, Float> expected = new LinkedHashMap<>();
      for (final Map.Entry<RID, Float> entry : matchesByScore("b").entrySet())
        if (expected.size() < 3)
          expected.put(entry.getKey(), entry.getValue());
      final Map<RID, Float> found = new LinkedHashMap<>();
      while (rs.hasNext()) {
        final Result row = rs.next();
        final RID rid = row.<Document>getProperty("node").getIdentity();
        found.put(rid, ((Number) row.getProperty("score")).floatValue());
        assertThat(found.get(rid)).isEqualTo(unfiltered.get(rid));
      }
      assertThat(found).containsExactlyEntriesOf(expected);
    }
  }

  @Test
  void cypherFilterOption() {
    createCorpus(1, false);
    final List<RID> some = new ArrayList<>(matchesByScore("b").keySet()).subList(0, 3);
    assertThat(cypherTenants("CALL db.index.fulltext.queryNodes('Doc[content]', 'common', {limit: " + WANTED
        + ", filter: $rids}) YIELD node RETURN node", Map.of("rids", some))).containsOnly("b").hasSize(3);
    assertThat(cypherTenants("CALL db.index.fulltext.queryNodes('Doc[content]', 'common', {filter: []}) YIELD node RETURN node",
        Map.of())).isEmpty();
    assertThatThrownBy(() -> cypherTenants("CALL db.index.fulltext.queryNodes('Doc[content]', 'common', {other: 1}) YIELD node "
        + "RETURN node", Map.of())).hasMessageContaining("where").hasMessageContaining("filter");
  }

  @Test
  void searchIndexRefusesAThirdArgument() {
    createCorpus(1, false);
    assertThatThrownBy(() -> tenantsOf("SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'common', {}) = true LIMIT 1"))
        .isInstanceOf(CommandSemanticException.class);
  }

  private void createCorpus(final int buckets, final boolean tenantIndex) {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc BUCKETS " + buckets);
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE PROPERTY Doc.tenant STRING");
      database.command("sql", "CREATE PROPERTY Doc.k INTEGER");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
      if (tenantIndex)
        database.command("sql", "CREATE INDEX ON Doc (tenant) NOTUNIQUE");
      for (int i = 0; i < DOCUMENTS; i++) {
        final boolean tenantB = i % (DOCUMENTS / TENANT_B_DOCUMENTS) == 0;
        // tenant b: one occurrence in a long text of varying length (lowest BM25, all different); tenant a: the term repeated
        database.newVertex("Doc").set("tenant", tenantB ? "b" : "a").set("k", i)
            .set("content", tenantB ? "common " + "filler ".repeat(20 + i % 37) : "common common common common").save();
      }
    });
    // The first search computes the index's corpus statistics with a scan of the type, which counts as reads: not the subject
    tenantsOf("SELECT FROM Doc WHERE " + SEARCH + " LIMIT 1");
  }

  /** The matches of the unfiltered search of tenant {@code tenant} (every one when null), best first. */
  private Map<RID, Float> matchesByScore(final String tenant) {
    final Map<RID, Float> matches = new LinkedHashMap<>();
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid, tenant, $score AS score FROM Doc WHERE " + SEARCH
        + " ORDER BY score DESC, rid")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        if (tenant == null || tenant.equals(row.getProperty("tenant")))
          matches.put(row.getProperty("rid"), ((Number) row.getProperty("score")).floatValue());
      }
    }
    return matches;
  }

  private void assertSameAnswerAsTheUnfilteredSearch(final String query, final Map<String, Object> parameters) {
    final Map<RID, Float> expected = new LinkedHashMap<>();
    for (final Map.Entry<RID, Float> entry : matchesByScore((String) parameters.get("t")).entrySet())
      if (expected.size() < WANTED)
        expected.put(entry.getKey(), entry.getValue());

    final Map<RID, Float> found = new LinkedHashMap<>();
    try (final ResultSet rs = database.query("sql", query, parameters)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        found.put(row.getProperty("rid"), ((Number) row.getProperty("score")).floatValue());
      }
    }
    assertThat(found).containsExactlyEntriesOf(expected);
  }

  private long recordsLoadedBy(final String query) {
    return recordsLoadedBy(query, Map.of());
  }

  private long recordsLoadedBy(final String query, final Map<String, Object> parameters) {
    final long before = readRecords();
    try (final ResultSet rs = database.query("sql", query, parameters)) {
      int rows = 0;
      while (rs.hasNext()) {
        rs.next();
        ++rows;
      }
      assertThat(rows).as(query).isEqualTo(WANTED);
    }
    return readRecords() - before;
  }

  private long readRecords() {
    return ((Number) ((DatabaseInternal) database).getStats().get("readRecord")).longValue();
  }

  private String plan(final String query) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + query)) {
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private List<String> tenantsOf(final String query) {
    final List<String> tenants = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext())
        tenants.add(rs.next().getProperty("tenant"));
    }
    return tenants;
  }

  private List<String> cypherTenants(final String query, final Map<String, Object> parameters) {
    final List<String> tenants = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, parameters)) {
      while (rs.hasNext())
        tenants.add(rs.next().<Document>getProperty("node").getString("tenant"));
    }
    return tenants;
  }
}
