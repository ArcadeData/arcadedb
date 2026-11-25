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
package com.arcadedb.function.sql.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The documented contract of {@code vector.neighbors} options ({@code maxDistance}, {@code groupBy} paths, argument checks, the
 * {@code filter} shapes), and of the score/RID shapes {@code vector.fuse} and {@code vector.mmr} read from their source rows, as
 * the vector.adoc reference states them. Written from the mutation survivors of the {@code com.arcadedb.function.sql.vector} lane:
 * every assertion below passes on main, each pins behaviour no other test did.
 */
class VectorSqlFunctionContractTest extends TestHelper {
  private final Map<String, RID> rids = new HashMap<>();

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE Pt");
    database.command("sql", "CREATE PROPERTY Pt.name STRING");
    database.command("sql", "CREATE PROPERTY Pt.src STRING");
    database.command("sql", "CREATE PROPERTY Pt.meta MAP");
    database.command("sql", "CREATE PROPERTY Pt.v ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON Pt (v) LSM_VECTOR METADATA { \"dimensions\": 2, \"similarity\": \"EUCLIDEAN\" }");
    database.transaction(() -> {
      add("a", 3f, 4f, "s1", Map.of("src", "m1", "deep", Map.of("src", "d1")));
      add("b", 6f, 8f, "s1", Map.of("src", "m1", "deep", Map.of("src", "d2")));
      add("c", 0f, 1f, "s2", Map.of("src", "m2", "deep", Map.of("src", "d1")));
      add("d", 1f, 0f, "s2", Map.of("src", "m3"));
      add("e", 2f, 2f, null, null);
      add("f", 5f, 5f, null, null);
    });
  }

  private void add(final String name, final float x, final float y, final String src, final Map<String, Object> meta) {
    final var doc = database.newDocument("Pt").set("name", name, "v", new float[] { x, y });
    if (src != null)
      doc.set("src", src);
    if (meta != null)
      doc.set("meta", meta);
    doc.save();
    rids.put(name, doc.getIdentity());
  }

  private List<Result> rows(final String sql, final Object... args) {
    final List<Result> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql, args)) {
      rs.forEachRemaining(out::add);
    }
    return out;
  }

  private List<String> names(final String nearOptions) {
    final List<String> out = new ArrayList<>();
    for (final Result r : rows("SELECT name FROM (SELECT expand(vector.neighbors('Pt[v]', [0.0, 0.0], 10" + nearOptions + ")))"))
      out.add(r.getProperty("name"));
    return out;
  }

  private float distanceOf(final String name) {
    for (final Result r : rows("SELECT name, distance FROM (SELECT expand(vector.neighbors('Pt[v]', [0.0, 0.0], 10)))"))
      if (name.equals(r.getProperty("name")))
        return ((Number) r.getProperty("distance")).floatValue();
    throw new AssertionError("no row for " + name);
  }

  // ---------------------------------------------------------------- maxDistance

  @Test
  void aRowExactlyAtMaxDistanceIsKeptAndOneBeyondItIsDropped() {
    // "Drops neighbors further than this distance": equal is not further
    final float atBound = distanceOf("a");
    assertThat(names(", { maxDistance: " + (double) atBound + " }")).contains("a").doesNotContain("b");
    assertThat(names(", { maxDistance: " + (double) Math.nextDown(atBound) + " }")).doesNotContain("a").contains("c", "d");
  }

  @Test
  void aNaNMaxDistanceIsRefused() {
    assertThatThrownBy(() -> rows("SELECT expand(vector.neighbors('Pt[v]', [0.0, 0.0], 3, { maxDistance: ? }))", Double.NaN))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("maxDistance must be a number, not NaN");
  }

  // ---------------------------------------------------------------- argument checks

  @Test
  void optionValuesAreChecked() {
    assertThatThrownBy(() -> names(", { groupBy: 'src', groupSize: 0 }")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("groupSize must be >= 1");
    assertThatThrownBy(() -> names(", { groupBy: 'src', groupSize: -2 }")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("groupSize must be >= 1");
    assertThatThrownBy(() -> names(", { rerank: -1 }")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("rerank must be >= 0");
    // 0 and absent are accepted: 0 keeps the scores as they are, which an unquantized index ignores
    assertThat(names(", { rerank: 0 }")).hasSize(6);
    assertThat(names(", { rerank: 3 }")).hasSize(6);
  }

  @Test
  void indexAndKeyAreChecked() {
    assertThatThrownBy(() -> rows("SELECT expand(vector.neighbors(?, [0.0, 0.0], 3))", (Object) null))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("index name is null");
    assertThatThrownBy(() -> rows("SELECT expand(vector.neighbors('Pt[v]', ?, 3))", (Object) null))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("key is null");
    assertThatThrownBy(() -> rows("SELECT expand(vector.neighbors('Pt[name]', [0.0, 0.0], 3))"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("No vector index found on property 'name'");
    assertThatThrownBy(() -> rows("SELECT expand(vector.neighbors('Pt[v]', 'no-such-vertex', 3))"))
        .isInstanceOf(CommandSQLParsingException.class);
  }

  @Test
  void kOfZeroAnswersNothingAndAListIsAVector() {
    assertThat(rows("SELECT expand(vector.neighbors('Pt[v]', [0.0, 0.0], 0))")).isEmpty();
    assertThat(rows("SELECT expand(vector.neighbors('Pt[v]', ?, 2))", List.of(0.0, 0.0))).hasSize(2);
  }

  @Test
  void theGroupedCandidateBudgetIsAnInclusiveBound() {
    // five candidates per member slot, capped at 100,000: 20,000 groups of 1 is exactly the cap
    assertThat(rows("SELECT expand(vector.neighbors('Pt[v]', [0.0, 0.0], 20000, { groupBy: 'name' }))")).hasSize(6);
    assertThatThrownBy(() -> rows("SELECT expand(vector.neighbors('Pt[v]', [0.0, 0.0], 20001, { groupBy: 'name' }))"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("over-fetch budget exceeded");
  }

  // ---------------------------------------------------------------- groupBy paths

  @Test
  void groupByReadsFlatAndNestedPaths() {
    // flat: s1 (a, b), s2 (c, d), and the records without the property share ONE null group (e, f)
    assertThat(names(", { groupBy: 'src' }")).hasSize(3);
    assertThat(names(", { groupBy: 'src', groupSize: 2 }")).hasSize(6);
    // nested one level: m1 (a, b), m2 (c), m3 (d), null (e, f)
    assertThat(names(", { groupBy: 'meta.src' }")).hasSize(4);
    assertThat(names(", { groupBy: 'meta.src', groupSize: 2 }")).hasSize(6);
    // nested two levels: d1 (a, c), d2 (b), and a missing segment or a missing record property is the null group (d, e, f)
    assertThat(names(", { groupBy: 'meta.deep.src' }")).hasSize(3);
    assertThat(names(", { groupBy: 'meta.deep.src', groupSize: 3 }")).hasSize(6);
    // a path that descends through a value that is not a map or a document reads as null for every record: one group
    assertThat(names(", { groupBy: 'src.deeper' }")).hasSize(1);
    // k is a number of groups: the nearest group of each source comes first, c (distance 1) leads
    assertThat(names(", { groupBy: 'src' }").get(0)).isIn("c", "d");
    assertThat(names(", { groupBy: 'meta.src' }").subList(0, 2)).containsExactlyInAnyOrder("c", "d");
  }

  // ---------------------------------------------------------------- filter shapes

  @Test
  void aFilterTakesRecordsRidsAndSingleColumnRows() {
    final Document a = rids.get("a").asDocument();
    final Document c = rids.get("c").asDocument();
    final Map<String, Object> params = new HashMap<>();
    params.put("docs", List.of(a, c));
    params.put("rids", List.of(rids.get("a"), rids.get("c")));
    params.put("strings", List.of(rids.get("a").toString(), rids.get("c").toString()));
    for (final String param : new String[] { "docs", "rids", "strings" }) {
      final List<String> got = new ArrayList<>();
      try (final ResultSet rs = database.query("sql",
          "SELECT name FROM (SELECT expand(vector.neighbors('Pt[v]', [0.0, 0.0], 10, { filter: :" + param + " })))", params)) {
        rs.forEachRemaining(r -> got.add(r.getProperty("name")));
      }
      assertThat(new TreeSet<>(got)).as(param).containsExactly("a", "c");
    }
    // the row of a subquery: a record, or one RID-valued column
    assertThat(rows("SELECT name FROM (SELECT expand(vector.neighbors('Pt[v]', [0.0, 0.0], 10, "
        + "{ filter: (SELECT FROM Pt WHERE name IN ['a', 'c']) })))")).hasSize(2);
    assertThat(rows("SELECT name FROM (SELECT expand(vector.neighbors('Pt[v]', [0.0, 0.0], 10, "
        + "{ filter: (SELECT @rid FROM Pt WHERE name IN ['a', 'c']) })))")).hasSize(2);
    // a row with several columns is a typo in the projection, and stays loud
    assertThatThrownBy(() -> rows("SELECT name FROM (SELECT expand(vector.neighbors('Pt[v]', [0.0, 0.0], 10, "
        + "{ filter: (SELECT @rid, name FROM Pt) })))")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("project the RID alone");
    assertThatThrownBy(() -> rows("SELECT name FROM (SELECT expand(vector.neighbors('Pt[v]', [0.0, 0.0], 10, { filter: [5] })))"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("must contain RIDs");
  }

  // ---------------------------------------------------------------- score and RID shapes of the source rows

  private List<Map<String, Object>> scored(final String key, final Object... nameAndValue) {
    final List<Map<String, Object>> list = new ArrayList<>();
    for (int i = 0; i < nameAndValue.length; i += 2) {
      final Map<String, Object> row = new HashMap<>();
      row.put("@rid", rids.get((String) nameAndValue[i]));
      row.put(key, nameAndValue[i + 1]);
      list.add(row);
    }
    return list;
  }

  private List<String> fuseNames(final String fusion, final Object first, final Object second) {
    final List<String> out = new ArrayList<>();
    final Map<String, Object> params = new HashMap<>();
    params.put("x", first);
    params.put("y", second);
    try (final ResultSet rs = database.query("sql", "SELECT name FROM (SELECT expand(vector.fuse(:x, :y, { fusion: '" + fusion + "' })))",
        params)) {
      rs.forEachRemaining(r -> out.add(r.getProperty("name")));
    }
    return out;
  }

  @Test
  void aDistanceFieldReadsAsALowerIsBetterScoreAndScoreAndDollarScoreAreHigherIsBetter() {
    // LINEAR min-max normalizes each source: with a distance the nearest row must win, with a score the highest
    final List<Map<String, Object>> byDistance = scored("distance", "a", 1.0f, "b", 9.0f);
    final List<Map<String, Object>> byDistance2 = scored("distance", "a", 2.0f, "b", 8.0f);
    assertThat(fuseNames("LINEAR", byDistance, byDistance2).get(0)).isEqualTo("a");

    final List<Map<String, Object>> byScore = scored("score", "a", 1.0f, "b", 9.0f);
    final List<Map<String, Object>> byScore2 = scored("score", "a", 2.0f, "b", 8.0f);
    assertThat(fuseNames("LINEAR", byScore, byScore2).get(0)).isEqualTo("b");

    final List<Map<String, Object>> byDollar = scored("$score", "a", 1.0f, "b", 9.0f);
    final List<Map<String, Object>> byDollar2 = scored("$score", "a", 2.0f, "b", 8.0f);
    assertThat(fuseNames("LINEAR", byDollar, byDollar2).get(0)).isEqualTo("b");

    // a row carrying no score at all cannot be normalized
    final List<Map<String, Object>> none = scored("other", "a", 1.0f, "b", 9.0f);
    assertThatThrownBy(() -> fuseNames("LINEAR", none, byScore)).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("requires a numeric score");
  }

  @Test
  void mmrReadsADistanceAsASimilarityOfTheOppositeSign() {
    // lambda 1: pure relevance, the nearest (lowest distance) is picked first, as with a score the highest is
    final Map<String, Object> params = new HashMap<>();
    params.put("rows", scored("distance", "a", 5.0f, "c", 1.0f, "b", 9.0f));
    params.put("scores", scored("score", "a", 5.0f, "c", 1.0f, "b", 9.0f));
    final List<String> byDistance = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT name FROM (SELECT expand(vector.mmr(:rows, 'v', { lambda: 1.0, k: 2 })))",
        params)) {
      rs.forEachRemaining(r -> byDistance.add(r.getProperty("name")));
    }
    assertThat(byDistance).containsExactly("c", "a");
    final List<String> byScore = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT name FROM (SELECT expand(vector.mmr(:scores, 'v', { lambda: 1.0, k: 2 })))",
        params)) {
      rs.forEachRemaining(r -> byScore.add(r.getProperty("name")));
    }
    assertThat(byScore).containsExactly("b", "a");
  }

  @Test
  void fuseTakesBareRecordsAndRowsWithAnIdentityAsItsSources() {
    // the rows of a plain SELECT carry their record: the RID is read from the row's identity, and a bare record or a RID works too
    final List<String> names = new ArrayList<>();
    final Map<String, Object> params = new HashMap<>();
    params.put("recs", List.of(rids.get("a").asDocument(), rids.get("b").asDocument()));
    params.put("ids", List.of(rids.get("b"), rids.get("c")));
    try (final ResultSet rs = database.query("sql",
        "SELECT name FROM (SELECT expand(vector.fuse(:recs, :ids, (SELECT FROM Pt WHERE name = 'e'))))", params)) {
      rs.forEachRemaining(r -> names.add(r.getProperty("name")));
    }
    // b is in two sources: first; the other rows follow, each present once
    assertThat(names).hasSize(4).startsWith("b").containsExactlyInAnyOrder("b", "a", "c", "e");
  }

  @Test
  void theRowsOfASubSelectAreReadLikeTheMapRows() {
    // the rows of `SELECT @rid, distance FROM (SELECT expand(vector.neighbors(...)))` are results, not maps: distance is still lower-is-better
    final String near = "(SELECT @rid AS rid, distance FROM (SELECT expand(vector.neighbors('Pt[v]', [0.0, 0.0], 3))))";
    final String far = "(SELECT @rid AS rid, distance FROM (SELECT expand(vector.neighbors('Pt[v]', [10.0, 10.0], 3))))";
    final List<String> byDistance = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT name FROM (SELECT expand(vector.fuse(" + near + ", " + far + ", { fusion: 'LINEAR', weights: [1, 0.0] })))")) {
      rs.forEachRemaining(r -> byDistance.add(r.getProperty("name")));
    }
    // weight 0 on the second source: the first source decides, its nearest record (c at 1, then d at 1, e at 8) leads
    assertThat(byDistance.get(0)).isIn("c", "d");
    assertThat(byDistance.subList(0, 2)).containsExactlyInAnyOrder("c", "d");

    // a `score` column is higher-is-better, a `$score` one too
    final List<String> byScore = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT name FROM (SELECT expand(vector.fuse("
        + "(SELECT @rid AS rid, 1.0 AS score FROM Pt WHERE name = 'a'), (SELECT @rid AS rid, 3.0 AS score FROM Pt WHERE name = 'b'), "
        + "{ fusion: 'LINEAR', weights: [1, 2] })))")) {
      rs.forEachRemaining(r -> byScore.add(r.getProperty("name")));
    }
    assertThat(byScore).containsExactly("b", "a");
  }
}
