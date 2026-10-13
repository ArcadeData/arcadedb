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
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9909: the {@code @rid} conditions ANDed with {@code SEARCH_INDEX} were a filter after the fetch, so every match was
 * loaded - and, since the filter was a residual, the LIMIT was not pushed down - before {@code @rid > :cursor} rejected
 * most of them. Keyset pagination over a full-text result paid for every match on every page.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9909RidConditionPushdownTest extends TestHelper {
  private static final int    DOCUMENTS = 1_200;
  private static final String SEARCH    = "SEARCH_INDEX('Doc[content]', 'common') = true";

  @ParameterizedTest
  @ValueSource(strings = { "BM25_1", "BM25_4", "CLASSIC_1", "CLASSIC_4" })
  void theRidConditionsAnswerWhatTheFilterAnswered(final String variant) {
    createCorpus(variant);
    final List<RID> ordered = rids("SELECT FROM Doc WHERE " + SEARCH + " ORDER BY $score DESC");
    assertThat(ordered).hasSize(DOCUMENTS);
    final List<RID> byRid = new ArrayList<>(ordered);
    byRid.sort(Comparator.naturalOrder());
    final RID cursor = byRid.get(DOCUMENTS / 3);
    final RID upper = byRid.get(2 * DOCUMENTS / 3);
    final List<RID> some = List.of(byRid.get(5), byRid.get(DOCUMENTS / 2), byRid.get(DOCUMENTS - 1));

    final Map<String, Object> params = new HashMap<>();
    params.put("cursor", cursor);
    params.put("upper", upper);
    params.put("some", some);

    assertSame("@rid > :cursor", r -> r.compareTo(cursor) > 0, ordered, params);
    assertSame("@rid >= :cursor", r -> r.compareTo(cursor) >= 0, ordered, params);
    assertSame("@rid < :cursor", r -> r.compareTo(cursor) < 0, ordered, params);
    assertSame("@rid <= :cursor", r -> r.compareTo(cursor) <= 0, ordered, params);
    assertSame("@rid > :cursor AND @rid < :upper", r -> r.compareTo(cursor) > 0 && r.compareTo(upper) < 0, ordered, params);
    assertSame("@rid <> :cursor", r -> !r.equals(cursor), ordered, params);
    assertSame("@rid NOT IN :some", r -> !some.contains(r), ordered, params);
    assertSame("@rid NOT IN [" + some.get(0) + ", " + some.get(1) + "]", r -> !r.equals(some.get(0)) && !r.equals(some.get(1)),
        ordered, params);
    // A literal and a RID's string spelling, which the comparison accepts too
    assertSame("@rid > " + cursor, r -> r.compareTo(cursor) > 0, ordered, params);
    assertSame("@rid > '" + cursor + "'", r -> r.compareTo(cursor) > 0, ordered, params);
    // Contradictory conditions: nothing to search for
    assertSame("@rid > :upper AND @rid < :cursor", r -> false, ordered, params);
    assertSame("@rid >= :cursor AND @rid < :cursor", r -> false, ordered, params);
    // Shapes decided by evaluating the condition per RID: an empty list, a list mixing RIDs and other values, a null bound
    assertSame("@rid NOT IN []", r -> true, ordered, params);
    assertSame("@rid NOT IN [" + some.get(0) + ", 'not a rid', 7]", r -> !r.equals(some.get(0)), ordered, params);
    params.put("nothing", null);
    assertSame("@rid > :nothing", r -> false, ordered, params);
    assertSame("@rid <> :nothing", r -> false, ordered, params);
    // IN, alone or with a range, takes the direct fetch by RID, as before, which answers in the list's order
    final List<RID> inAndRange = new ArrayList<>();
    for (final RID rid : some)
      if (rid.compareTo(cursor) > 0)
        inAndRange.add(rid);
    assertThat(rids("SELECT FROM Doc WHERE @rid IN :some AND @rid > :cursor AND " + SEARCH, params))
        .containsExactlyInAnyOrderElementsOf(inAndRange);
  }

  @ParameterizedTest
  @ValueSource(strings = { "BM25_1", "BM25_4", "CLASSIC_4" })
  void theRidConditionsAreDecidedBeforeTheRecordsAreLoaded(final String variant) {
    createCorpus(variant);
    final List<RID> byRid = rids("SELECT FROM Doc WHERE " + SEARCH + " ORDER BY @rid");
    final RID cursor = byRid.get(DOCUMENTS - 50);
    // The first search computes the index's corpus statistics with a scan of the type: not the subject
    rids("SELECT FROM Doc WHERE " + SEARCH + " LIMIT 1");

    final String filtered = "SELECT FROM Doc WHERE @rid > :cursor AND " + SEARCH;
    // Decided by the fetch, and no longer by the filter after it, which evaluates the search's own condition only
    final String plan = plan(filtered, cursor);
    assertThat(plan).contains("RID FILTER (@rid > :cursor)");
    assertThat(plan.indexOf("@rid > :cursor")).isEqualTo(plan.lastIndexOf("@rid > :cursor"));
    assertThat(recordsLoadedBy(filtered, cursor)).isEqualTo(49);

    // No residual filter left: the LIMIT is pushed down again
    final String limited = "SELECT FROM Doc WHERE @rid > :cursor AND " + SEARCH + " LIMIT 10";
    assertThat(plan(limited, cursor)).contains("(best 10 needed)");
    assertThat(recordsLoadedBy(limited, cursor)).isEqualTo(10);
  }

  @ParameterizedTest
  @ValueSource(strings = { "BM25_1", "BM25_4", "CLASSIC_1", "CLASSIC_4" })
  void keysetPaginationInRidOrderLoadsOnlyEachPage(final String variant) {
    createCorpus(variant);
    final List<RID> byRid = rids("SELECT FROM Doc WHERE " + SEARCH + " ORDER BY @rid");
    final List<RID> expected = new ArrayList<>(byRid);
    expected.sort(Comparator.naturalOrder());
    assertThat(byRid).isEqualTo(expected);
    rids("SELECT FROM Doc WHERE " + SEARCH + " LIMIT 1");

    final String page = "SELECT @rid AS rid, $score AS score FROM Doc WHERE @rid > :cursor AND " + SEARCH + " ORDER BY @rid LIMIT 100";
    final String planned = plan(page, new RID(-1, -1));
    assertThat(planned).contains("ORDER BY @rid ASC (first 100 needed)").doesNotContain("ORDER BY @rid ASC\n").doesNotContain("+ ORDER BY");

    final Map<Object, Float> scores = new HashMap<>();
    for (final Result row : rows("SELECT @rid AS rid, $score AS score FROM Doc WHERE " + SEARCH, Map.of()))
      scores.put(row.getProperty("rid"), row.<Float>getProperty("score"));

    final List<RID> paged = new ArrayList<>();
    RID cursor = new RID(-1, -1);
    while (true) {
      final long before = readRecords();
      final List<Result> rows = rows(page, Map.of("cursor", cursor));
      if (rows.isEmpty())
        break;
      // Each page loads its own rows, and nothing past them
      assertThat(readRecords() - before).isEqualTo(rows.size());
      for (final Result row : rows) {
        final RID rid = row.getProperty("rid");
        assertThat(row.<Float>getProperty("score")).isEqualTo(scores.get(rid));
        paged.add(rid);
      }
      cursor = paged.getLast();
    }
    assertThat(paged).isEqualTo(byRid);

    // Descending, from the end, and a limit past the last match
    final List<RID> descending = rids("SELECT FROM Doc WHERE @rid < :cursor AND " + SEARCH + " ORDER BY @rid DESC LIMIT 5",
        Map.of("cursor", byRid.getLast()));
    assertThat(descending).isEqualTo(byRid.reversed().subList(1, 6));
    assertThat(rids("SELECT FROM Doc WHERE @rid < :cursor AND " + SEARCH + " ORDER BY @rid DESC LIMIT 50", Map.of("cursor",
        byRid.get(9)))).isEqualTo(byRid.subList(0, 9).reversed());

    // SKIP is served out of the same RID order
    final String skipped = "SELECT FROM Doc WHERE @rid > :cursor AND " + SEARCH + " ORDER BY @rid SKIP 3 LIMIT 5";
    assertThat(plan(skipped, new RID(-1, -1))).contains("ORDER BY @rid ASC (first 8 needed)");
    assertThat(rids(skipped, Map.of("cursor", new RID(-1, -1)))).isEqualTo(byRid.subList(3, 8));

    // An order on more than @rid is sorted after the fetch as before
    final String twoKeys = "SELECT FROM Doc WHERE @rid > :cursor AND " + SEARCH + " ORDER BY @rid, n LIMIT 5";
    assertThat(plan(twoKeys, new RID(-1, -1))).doesNotContain("ORDER BY @rid ASC (").contains("ORDER BY");
    assertThat(rids(twoKeys, Map.of("cursor", new RID(-1, -1)))).isEqualTo(byRid.subList(0, 5));
  }

  @ParameterizedTest
  @ValueSource(strings = { "BM25_4", "CLASSIC_4" })
  void ridOrderIsOneOrderAcrossTheBucketsOfTheHierarchy(final String variant) {
    createCorpus(variant);
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE SubDoc EXTENDS Doc BUCKETS 3");
      for (int i = 0; i < 60; i++)
        database.newDocument("SubDoc").set("n", DOCUMENTS + i).set("content", "common filler").save();
    });
    final List<RID> all = new ArrayList<>(rids("SELECT FROM Doc WHERE " + SEARCH));
    all.sort(Comparator.naturalOrder());
    assertThat(all).hasSize(DOCUMENTS + 60);

    final String descending = "SELECT FROM Doc WHERE @rid > :cursor AND " + SEARCH + " ORDER BY @rid DESC LIMIT 25";
    assertThat(plan(descending, new RID(-1, -1))).contains("ORDER BY @rid DESC (first 25 needed)");
    assertThat(rids(descending, Map.of("cursor", new RID(-1, -1)))).isEqualTo(all.reversed().subList(0, 25));
    assertThat(rids("SELECT FROM Doc WHERE @rid > :cursor AND " + SEARCH + " ORDER BY @rid LIMIT 25", Map.of("cursor",
        all.get(100)))).isEqualTo(all.subList(101, 126));
  }

  @ParameterizedTest
  @ValueSource(strings = { "BM25_1", "CLASSIC_4" })
  void aMatchWhoseRecordIsGoneIsSkippedInRidOrder(final String variant) {
    createCorpus(variant);
    final List<RID> byRid = rids("SELECT FROM Doc WHERE " + SEARCH + " ORDER BY @rid");
    // Removed from its bucket behind the index's back: the index still answers it, the load does not
    final RID gone = byRid.get(3);
    database.transaction(() -> ((DatabaseInternal) database).getSchema().getBucketById(gone.getBucketId()).deleteRecord(gone));

    final List<RID> alive = new ArrayList<>(byRid);
    alive.remove(gone);
    final String page = "SELECT FROM Doc WHERE @rid > :cursor AND " + SEARCH + " ORDER BY @rid LIMIT 10";
    assertThat(rids(page, Map.of("cursor", new RID(-1, -1)))).isEqualTo(alive.subList(0, 10));
    assertThat(rids(page.replace("ORDER BY @rid", "ORDER BY @rid DESC"), Map.of("cursor", byRid.get(0))))
        .isEqualTo(alive.reversed().subList(0, 10));

    // The dangling index entry and the bucket counter were left wrong on purpose: drop them before the integrity check
    database.getSchema().dropType("Doc");
  }

  @Test
  void conditionsTheFunctionCannotDecideStayInTheFilter() {
    createCorpus("BM25_4");
    // A residual condition on a property keeps its filter; the @rid conjunct is still pushed
    final String mixed = "SELECT FROM Doc WHERE @rid > :cursor AND n % 2 = 0 AND " + SEARCH;
    final RID cursor = new RID(-1, -1);
    assertThat(plan(mixed, cursor)).contains("RID FILTER (@rid > :cursor)").contains("n % 2 = 0");
    assertThat(rids(mixed, Map.of("cursor", cursor))).hasSize(DOCUMENTS / 2);

    // A right-hand side that reads the record is not decidable on the RID
    final String perRecord = "SELECT FROM Doc WHERE @rid > link AND " + SEARCH;
    assertThat(plan(perRecord, cursor)).doesNotContain("RID FILTER");

    // A variable a per-record LET computes after the fetch changes from row to row: the condition stays in the filter
    final String perRecordLet = "SELECT FROM Doc LET $self = @rid WHERE @rid >= $self AND " + SEARCH;
    assertThat(plan(perRecordLet, cursor)).doesNotContain("RID FILTER");
    assertThat(rids(perRecordLet, Map.of())).hasSize(DOCUMENTS);
    assertThat(rids(perRecordLet.replace(">=", ">"), Map.of())).isEmpty();

    // An OR: each branch pushes its own conditions
    final List<RID> byRid = rids("SELECT FROM Doc WHERE " + SEARCH + " ORDER BY @rid");
    final RID first = byRid.get(10);
    final RID last = byRid.get(DOCUMENTS - 10);
    final List<RID> union = rids("SELECT FROM Doc WHERE (@rid < :first AND " + SEARCH + ") OR (@rid > :last AND " + SEARCH + ")",
        Map.of("first", first, "last", last));
    assertThat(union).hasSize(10 + 9);

    // Each branch remembers the matches it fetched: the rows of both are answered from their union, with the scores of the
    // complete search
    final Map<Object, Float> scores = new HashMap<>();
    for (final Result row : rows("SELECT @rid AS rid, $score AS score FROM Doc WHERE " + SEARCH, Map.of()))
      scores.put(row.getProperty("rid"), row.<Float>getProperty("score"));
    final List<Result> unionRows = rows("SELECT @rid AS rid, $score AS score FROM Doc WHERE (@rid < :first AND " + SEARCH
        + ") OR (@rid > :last AND " + SEARCH + ")", Map.of("first", first, "last", last));
    assertThat(unionRows).hasSize(10 + 9);

    // Only one branch has an @rid condition: it pushes it, the other searches unfiltered
    final List<RID> oneBranch = rids("SELECT FROM Doc WHERE (@rid < :first AND " + SEARCH + ") OR (" + SEARCH + " AND n < 0)",
        Map.of("first", first));
    assertThat(oneBranch).containsExactlyInAnyOrderElementsOf(byRid.subList(0, 10));

    // A variable a global LET computes is known before the fetch: pushed, with the answer of the parameter it holds
    final String globalLet = "SELECT FROM Doc LET $c = :cursor WHERE @rid > $c AND " + SEARCH;
    assertThat(plan(globalLet, first)).contains("RID FILTER (@rid > $c)");
    assertThat(rids(globalLet, Map.of("cursor", last))).containsExactlyInAnyOrderElementsOf(byRid.subList(DOCUMENTS - 9, DOCUMENTS));
    for (final Result row : unionRows)
      assertThat(row.<Float>getProperty("score")).isEqualTo(scores.get(row.<Object>getProperty("rid")));
  }

  @Test
  void matchPushesTheRidConditionsOfTheRootNode() {
    createCorpus("BM25_4");
    final List<RID> byRid = rids("SELECT FROM Doc WHERE " + SEARCH + " ORDER BY @rid");
    final RID cursor = byRid.get(DOCUMENTS - 30);
    rids("SELECT FROM Doc WHERE " + SEARCH + " LIMIT 1");

    final String match = "MATCH {type: Doc, as: d, where: (@rid > :cursor AND " + SEARCH + ")} RETURN d.@rid AS rid";
    final long before = readRecords();
    final List<Object> matched = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", match, Map.of("cursor", cursor))) {
      while (rs.hasNext())
        matched.add(rs.next().getProperty("rid"));
    }
    assertThat(matched).containsExactlyInAnyOrderElementsOf(byRid.subList(DOCUMENTS - 29, DOCUMENTS));
    // MATCH loads the records it returns, and not the matches the cursor excludes
    assertThat(readRecords() - before).isLessThan(DOCUMENTS / 4);
  }

  @Test
  void geospatialPredicatesDecideTheRidConditionsBeforeLoading() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Place BUCKETS 4");
      database.command("sql", "CREATE PROPERTY Place.coords STRING");
      database.command("sql", "CREATE INDEX ON Place (coords) GEOSPATIAL");
      for (int i = 0; i < 200; i++)
        database.newDocument("Place").set("coords", "POINT(" + (10 + (i % 10) * 0.01) + " " + (45 + (i / 10) * 0.01) + ")").save();
    });
    final String within = "geo.within(coords, geo.geomFromText('POLYGON((9 44, 11 44, 11 46, 9 46, 9 44))')) = true";
    final List<RID> all = rids("SELECT FROM Place WHERE " + within + " ORDER BY @rid");
    assertThat(all).hasSize(200);
    final RID cursor = all.get(150);

    final String query = "SELECT FROM Place WHERE @rid > :cursor AND " + within;
    assertThat(plan(query, cursor)).contains("RID FILTER (@rid > :cursor)");
    final List<RID> after = rids(query + " ORDER BY @rid", Map.of("cursor", cursor));
    assertThat(after).isEqualTo(all.subList(151, 200));
    final long before = readRecords();
    assertThat(rids(query, Map.of("cursor", cursor))).hasSize(49);
    assertThat(readRecords() - before).isEqualTo(49);

    // '= false' asks for the places outside the shape, which the index cannot answer: the type is scanned
    final String westHalf = "geo.within(coords, geo.geomFromText('POLYGON((9 44, 10.045 44, 10.045 46, 9 46, 9 44))'))";
    assertThat(rids("SELECT FROM Place WHERE " + westHalf + " = true")).hasSize(100);
    assertThat(plan("SELECT FROM Place WHERE " + westHalf + " = false", cursor)).doesNotContain("INDEXED FUNCTION");
    assertThat(rids("SELECT FROM Place WHERE " + westHalf + " = false")).hasSize(100);
  }

  private void assertSame(final String ridCondition, final Predicate<RID> accepted, final List<RID> ordered,
      final Map<String, Object> params) {
    final List<RID> expected = new ArrayList<>();
    for (final RID rid : ordered)
      if (accepted.test(rid))
        expected.add(rid);

    final String pushed = "SELECT FROM Doc WHERE " + ridCondition + " AND " + SEARCH;
    assertThat(rids(pushed, params)).as(pushed).isEqualTo(expected);
    assertThat(rids(pushed + " ORDER BY $score DESC", params)).as(pushed).isEqualTo(expected);
    assertThat(rids(pushed + " LIMIT 7", params)).as(pushed + " LIMIT 7").isEqualTo(expected.subList(0, Math.min(7, expected.size())));
    final List<RID> expectedByRid = new ArrayList<>(expected);
    expectedByRid.sort(Comparator.naturalOrder());
    assertThat(rids(pushed + " ORDER BY @rid LIMIT 7", params)).as(pushed + " ORDER BY @rid LIMIT 7")
        .isEqualTo(expectedByRid.subList(0, Math.min(7, expectedByRid.size())));
    assertThat(rids(pushed + " ORDER BY @rid DESC", params)).as(pushed + " ORDER BY @rid DESC").isEqualTo(expectedByRid.reversed());

    // The same scores as without the pushdown
    final Map<Object, Float> scores = new HashMap<>();
    for (final Result row : rows("SELECT @rid AS rid, $score AS score FROM Doc WHERE " + SEARCH, Map.of()))
      scores.put(row.getProperty("rid"), row.<Float>getProperty("score"));
    for (final Result row : rows("SELECT @rid AS rid, $score AS score FROM Doc WHERE " + ridCondition + " AND " + SEARCH, params))
      assertThat(row.<Float>getProperty("score")).as(pushed).isEqualTo(scores.get(row.<Object>getProperty("rid")));
    // Also on the limit path, where each bucket keeps only its best accepted matches and the plan answers from them
    final List<Result> limitedRows = rows("SELECT @rid AS rid, $score AS score FROM Doc WHERE " + ridCondition + " AND " + SEARCH
        + " LIMIT 7", params);
    assertThat(limitedRows).hasSize(Math.min(7, expected.size()));
    for (int i = 0; i < limitedRows.size(); i++) {
      assertThat(limitedRows.get(i).<Object>getProperty("rid")).as(pushed + " LIMIT 7").isEqualTo(expected.get(i));
      assertThat(limitedRows.get(i).<Float>getProperty("score")).as(pushed + " LIMIT 7")
          .isEqualTo(scores.get(limitedRows.get(i).<Object>getProperty("rid")));
    }

    // And what a scan with the same condition answers
    assertThat(rids("SELECT FROM Doc WHERE " + ridCondition + " AND content LIKE '%common%' ORDER BY @rid", params)).as(ridCondition)
        .isEqualTo(expectedByRid);
  }

  private void createCorpus(final String variant) {
    final String similarity = variant.substring(0, variant.indexOf('_'));
    final int buckets = Integer.parseInt(variant.substring(variant.indexOf('_') + 1));
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS " + buckets);
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE PROPERTY Doc.n INTEGER");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"" + similarity + "\"}");
      for (int i = 0; i < DOCUMENTS; i++)
        // varying term frequency and document length, so scores differ and also tie
        database.newDocument("Doc").set("n", i).set("link", new RID(0, i))
            .set("content", "common " + "common ".repeat(i % 5) + "filler ".repeat(i % 3)).save();
    });
  }

  private long recordsLoadedBy(final String query, final RID cursor) {
    final long before = readRecords();
    try (final ResultSet rs = database.query("sql", query, Map.of("cursor", cursor))) {
      while (rs.hasNext())
        rs.next();
    }
    return readRecords() - before;
  }

  private long readRecords() {
    return ((Number) ((DatabaseInternal) database).getStats().get("readRecord")).longValue();
  }

  private String plan(final String query, final RID cursor) {
    try (final ResultSet rs = database.query("sql", query, Map.of("cursor", cursor))) {
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private List<RID> rids(final String query) {
    return rids(query, Map.of());
  }

  private List<RID> rids(final String query, final Map<String, Object> params) {
    final List<RID> rids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query, params)) {
      while (rs.hasNext())
        rids.add(rs.next().getIdentity().orElseThrow());
    }
    return rids;
  }

  private List<Result> rows(final String query, final Map<String, Object> params) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query, params)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }
}
