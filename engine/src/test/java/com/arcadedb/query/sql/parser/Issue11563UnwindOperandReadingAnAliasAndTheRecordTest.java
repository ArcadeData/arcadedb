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
package com.arcadedb.query.sql.parser;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #11563: under a narrowing projection, an {@code UNWIND <operand> AS x} whose operand reads a
 * name only the projected row holds (a renamed projection alias, an earlier UNWIND item) AND needs the record itself
 * ({@code @rid}, {@code out()}) read the record part as {@code null}, because it was evaluated against the projected row
 * alone. The #11537 fix covered only an operand that reads no such name.
 */
class Issue11563UnwindOperandReadingAnAliasAndTheRecordTest extends TestHelper {
  private RID aliceRid;
  private RID docRid;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("P");
      database.getSchema().createEdgeType("K");
      final MutableVertex alice = database.newVertex("P").set("name", "alice").save();
      final MutableVertex bob = database.newVertex("P").set("name", "bob").save();
      final MutableVertex carol = database.newVertex("P").set("name", "carol").save();
      alice.newEdge("K", bob).save();
      alice.newEdge("K", carol).save();
      aliceRid = alice.getIdentity();

      database.getSchema().createDocumentType("T");
      docRid = database.newDocument("T").set("name", "n1").set("tags", List.of("t1", "t2")).set("b", "q").save().getIdentity();
    });
  }

  private List<Result> rows(final String sql) {
    final List<Result> list = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      rs.forEachRemaining(list::add);
    }
    return list;
  }

  @Test
  void anOperandReadingAnEarlierUnwindItemAndARecordAttribute() {
    final List<Result> result = rows("SELECT name FROM T UNWIND tags AS x, [x, @rid] AS y");
    assertThat(result).hasSize(4);
    assertThat(result.stream().map(r -> r.getProperty("x") + ":" + r.getProperty("y")).toList())
        .containsExactly("t1:t1", "t1:" + docRid, "t2:t2", "t2:" + docRid);
    assertThat(result.getFirst().getPropertyNames()).containsExactlyInAnyOrder("name", "x", "y");
  }

  @Test
  void anOperandReadingARenamedProjectionAliasAndARecordAttribute() {
    // `name` is the projected `tags` (not the record's "n1"), `@rid` is the record's
    final List<Result> result = rows("SELECT tags AS name FROM T UNWIND [name, @rid] AS x");
    assertThat(result).hasSize(2);
    assertThat(result.get(0).<Object>getProperty("x")).isEqualTo(List.of("t1", "t2"));
    assertThat(result.get(1).<Object>getProperty("x")).isEqualTo(docRid);
    assertThat(result.getFirst().getPropertyNames()).containsExactlyInAnyOrder("name", "x");
  }

  @Test
  void aRecordLevelFunctionWhosePropertyNameSpellsAComputedAlias() {
    // `name` after the dot is the edge's vertex property, not a row read, but the textual check sees it as the alias
    final List<Result> result = rows("SELECT name.toUpperCase() AS name FROM P WHERE name = 'alice' UNWIND out('K').name AS x");
    assertThat(result.stream().map(r -> r.<String>getProperty("x")).toList()).containsExactlyInAnyOrder("bob", "carol");
    assertThat(result.getFirst().<String>getProperty("name")).isEqualTo("ALICE");
    assertThat(result.getFirst().getPropertyNames()).containsExactlyInAnyOrder("name", "x");
  }

  @Test
  void aRecordLevelFunctionNextToARenamedAlias() {
    final List<Result> result = rows("SELECT name AS n FROM P WHERE name = 'alice' UNWIND [n, out('K').size(), @rid] AS x");
    assertThat(result.stream().map(r -> r.<Object>getProperty("x")).toList()).containsExactly("alice", 2, aliceRid);
    assertThat(result.getFirst().getPropertyNames()).containsExactlyInAnyOrder("n", "x");
  }

  @Test
  void aProjectedAliasShadowsTheRecordFieldOfTheSameName() {
    // the projection writes `b` as null: the operand reads that null, not the record's "q"
    final List<Result> result = rows("SELECT null AS b FROM T UNWIND tags AS x, [x, b, @rid] AS y");
    assertThat(result.stream().map(r -> r.getProperty("x") + ":" + r.getProperty("y")).toList())
        .containsExactly("t1:t1", "t1:null", "t1:" + docRid, "t2:t2", "t2:null", "t2:" + docRid);
  }

  @Test
  void anExcludedNameIsNotReadBackFromTheRecord() {
    // `!b` drops the column: the operand reads null for it, as it does on the row, never the record's "q"
    final List<Result> result = rows("SELECT *, !b FROM T UNWIND tags AS x, [x, b, @rid] AS y");
    assertThat(result.stream().map(r -> r.getProperty("x") + ":" + r.getProperty("y")).toList())
        .containsExactly("t1:t1", "t1:null", "t1:" + docRid, "t2:t2", "t2:null", "t2:" + docRid);
    assertThat(result.getFirst().getPropertyNames()).doesNotContain("b").noneMatch(GeneratedAlias::is);
  }

  @Test
  void aStarProjectionDropsTheGeneratedColumnsAndKeepsItsExclusions() {
    // `tags AS x` is evaluated early (#11537): under `*` its generated column and the excluded `b` used to reach the row
    final List<Result> result = rows("SELECT *, !b FROM T UNWIND tags AS x");
    assertThat(result.stream().map(r -> r.<String>getProperty("x")).toList()).containsExactly("t1", "t2");
    for (final Result row : result) {
      assertThat(row.getPropertyNames()).contains("name", "tags", "x").doesNotContain("b").noneMatch(GeneratedAlias::is);
      assertThat(row.toJSON().keySet()).doesNotContain("b").noneMatch(GeneratedAlias::is);
    }
  }

  @Test
  void theCarriedRecordNeverReachesTheResult() {
    final List<Result> result = rows("SELECT name AS n FROM P WHERE name = 'alice' UNWIND [n, @rid] AS x");
    assertThat(result).hasSize(2);
    for (final Result row : result) {
      assertThat(row.getPropertyNames()).containsExactlyInAnyOrder("n", "x");
      assertThat(row.toJSON().keySet()).noneMatch(GeneratedAlias::is);
    }
  }

  @Test
  void anAggregatedRowStillReadsOnlyTheProjectedRow() {
    final List<Result> result = rows("SELECT name, count(*) AS c FROM T GROUP BY name UNWIND [name, c] AS x");
    assertThat(result.stream().map(r -> String.valueOf(r.<Object>getProperty("x"))).toList()).containsExactly("n1", "1");
    assertThat(result.getFirst().getPropertyNames()).containsExactlyInAnyOrder("name", "c", "x");
  }

  @Test
  void aMatchReturnRowIsNotARecord() {
    final List<Result> result = rows("MATCH {type: T, as: t} RETURN t.name AS n, t.tags AS g UNWIND g AS x, [x, n] AS y");
    assertThat(result.stream().map(r -> r.getProperty("x") + ":" + r.getProperty("y")).toList())
        .containsExactly("t1:t1", "t1:n1", "t2:t2", "t2:n1");
    assertThat(result.getFirst().getPropertyNames()).containsExactlyInAnyOrder("n", "g", "x", "y");
  }
}
