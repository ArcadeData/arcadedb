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
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10217: since #10181 / #10185 {@code vector.rerank}, {@code vector.mmr} and {@code vector.fuse} reuse the record a
 * candidate row carries instead of loading it. A row held in a variable before a write of the same transaction holds the record
 * as it was: a record deleted since came back in the result, and an updated one with its old values.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10217HeldRowsAfterWriteTest extends TestHelper {
  private static final String DELETE = "DELETE FROM Doc WHERE name = 'a'";
  private static final String UPDATE = "UPDATE Doc SET name = 'a2' WHERE name = 'a'";

  private static final String MMR    = "vector.mmr($h, 'emb', {lambda: 0.5, k: 5})";
  private static final String FUSE   = "vector.fuse($h, $h, {fusion: 'RRF', limit: 5})";
  private static final String RERANK = "vector.rerank($h, [1.0, 0.0, 0.0], 'emb', 5)";
  private static final String BOOST  = "vector.mmr(vector.boost($h, {boosts: [{field: 'score', weight: 0}]}), 'emb', {lambda: 0.5, k: 5})";

  @Override
  public void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc");
    database.command("sql", "CREATE PROPERTY Doc.name STRING");
    database.command("sql", "CREATE PROPERTY Doc.emb ARRAY_OF_FLOATS");
    database.command("sql", "CREATE PROPERTY Doc.score FLOAT");
  }

  private void reset() {
    database.transaction(() -> {
      database.command("sql", "DELETE FROM Doc");
      database.command("sql", "INSERT INTO Doc SET name = 'a', score = 1.0, emb = [1.0, 0.0, 0.0]");
      database.command("sql", "INSERT INTO Doc SET name = 'b', score = 0.9, emb = [0.9, 0.1, 0.0]");
      database.command("sql", "INSERT INTO Doc SET name = 'c', score = 0.1, emb = [0.0, 1.0, 0.0]");
    });
  }

  /** Holds the rows of Doc in $h (as {@code held} produces them), makes one write, then passes $h to {@code call}. */
  private List<String> run(final String held, final String write, final String call) {
    reset();
    final String script = "BEGIN;\nLET $h = " + held + ";\n" + write + ";\nLET $r = SELECT expand(" + call + ");\nCOMMIT;\nRETURN $r;";
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.command("sqlscript", script)) {
      rs.stream().forEach(r -> names.add(r.<String>getProperty("name")));
    }
    names.sort(null);
    return names;
  }

  private List<String> run(final String write, final String call) {
    return run("SELECT FROM Doc", write, call);
  }

  @Test
  void aRecordDeletedAfterTheRowsWereHeldIsNotReturned() {
    assertThat(run(DELETE, MMR)).containsExactly("b", "c");
    assertThat(run(DELETE, FUSE)).containsExactly("b", "c");
    assertThat(run(DELETE, RERANK)).containsExactly("b", "c");
    assertThat(run(DELETE, BOOST)).containsExactly("b", "c");
  }

  @Test
  void aRecordUpdatedAfterTheRowsWereHeldIsReturnedWithItsNewValues() {
    assertThat(run(UPDATE, MMR)).containsExactly("a2", "b", "c");
    assertThat(run(UPDATE, FUSE)).containsExactly("a2", "b", "c");
    assertThat(run(UPDATE, RERANK)).containsExactly("a2", "b", "c");
    assertThat(run(UPDATE, BOOST)).containsExactly("a2", "b", "c");
  }

  /** The output of a vector function held in a variable carries its records too, so it is checked the same way. */
  @Test
  void theHeldOutputOfAVectorFunctionIsCheckedToo() {
    final String fused = "SELECT expand(vector.fuse((SELECT FROM Doc), (SELECT FROM Doc), {fusion: 'RRF', limit: 5}))";
    assertThat(run(fused, DELETE, MMR)).containsExactly("b", "c");
    assertThat(run(fused, UPDATE, RERANK)).containsExactly("a2", "b", "c");
  }

  /** An updated embedding is scored with its new value: {@code a} moved away from the query vector and ranks last. */
  @Test
  void theScoreIsComputedOnTheNewValues() {
    reset();
    final String script = "BEGIN;\nLET $h = SELECT FROM Doc;\nUPDATE Doc SET emb = [-1.0, 0.0, 0.0] WHERE name = 'a';\n"
        + "LET $r = SELECT expand(vector.rerank($h, [1.0, 0.0, 0.0], 'emb', 5));\nCOMMIT;\nRETURN $r;";
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.command("sqlscript", script)) {
      rs.stream().forEach(r -> names.add(r.<String>getProperty("name")));
    }
    assertThat(names.getLast()).isEqualTo("a");
  }

  /** A transaction that wrote nothing still reuses every carried record: the check is two hash probes, not a load. */
  @Test
  void aTransactionWithoutWritesStillLoadsNothing() {
    reset();
    database.transaction(() -> {
      final List<Object> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT FROM Doc")) {
        rs.stream().forEach(r -> rows.add(new ScoredRecordRow((Document) r.getElement().get(), "score", 1.0f)));
      }
      final BasicCommandContext ctx = new BasicCommandContext();
      ctx.setDatabase(database);

      final long before = ((Number) database.getStats().get("readRecord")).longValue();
      final List<?> out = (List<?>) new SQLFunctionVectorMmr().execute(null, null, null,
          new Object[] { rows, "emb", Map.of("lambda", 0.5, "k", 5) }, ctx);

      assertThat(((Number) database.getStats().get("readRecord")).longValue() - before).isZero();
      assertThat(out).hasSize(3);
    });
  }
}
