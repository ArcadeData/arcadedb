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
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10169: restricting {@code vector.neighbors} by a property took a {@code filter} subquery listing every RID in scope,
 * which loads every record in scope and builds a set of them on every call. The {@code where} option tests the predicate on
 * each candidate the search meets instead, as an equality map or as a SQL condition, on {@code vector.neighbors} and
 * {@code vector.sparseNeighbors} alike.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10169VectorNeighborsWherePredicateTest extends TestHelper {
  private static final int     RECORDS = 600;
  private static final float[] QUERY   = { 1.0f, 0.2f, 0.1f, 0.0f };

  private void seedDense() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Passage BUCKETS 2");
      database.command("sql", "CREATE PROPERTY Passage.space STRING");
      database.command("sql", "CREATE PROPERTY Passage.year INTEGER");
      database.command("sql", "CREATE PROPERTY Passage.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Passage (embedding) LSM_VECTOR METADATA { dimensions: 4, similarity: 'COSINE' }");
    });
    final Random random = new Random(10169);
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument("Passage")
            .set("space", i % 3 == 0 ? "a" : "b")
            .set("year", 2000 + i % 25)
            .set("embedding", new float[] { random.nextFloat(), random.nextFloat(), random.nextFloat(), random.nextFloat() })
            .save();
    });
  }

  private List<Result> query(final String sql, final Object... args) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql, args)) {
      rs.forEachRemaining(rows::add);
    }
    return rows;
  }

  private List<Result> query(final String sql, final Map<String, Object> params) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql, params)) {
      rs.forEachRemaining(rows::add);
    }
    return rows;
  }

  private static List<RID> rids(final List<Result> rows) {
    final List<RID> out = new ArrayList<>(rows.size());
    for (final Result r : rows)
      out.add(r.getProperty("@rid"));
    return out;
  }

  private long readRecords() {
    return ((Number) database.getStats().get("readRecord")).longValue();
  }

  @Test
  void anEqualityMapAnswersWhatTheFilterSubqueryAnswers() {
    seedDense();

    final long beforeFilter = readRecords();
    final List<Result> byFilter = query("""
        SELECT expand(`vector.neighbors`('Passage[embedding]', ?, 10,
          { filter: (SELECT @rid FROM Passage WHERE space = 'a').@rid }))""", QUERY);
    final long filterLoads = readRecords() - beforeFilter;

    final long beforeWhere = readRecords();
    final List<Result> byWhere = query("""
        SELECT expand(`vector.neighbors`('Passage[embedding]', :q, 10, { where: { space: :space } }))""",
        Map.of("q", QUERY, "space", "a"));
    final long whereLoads = readRecords() - beforeWhere;

    assertThat(byWhere).hasSize(10);
    for (final Result r : byWhere)
      assertThat(r.<String>getProperty("space")).isEqualTo("a");
    assertThat(rids(byWhere)).containsExactlyElementsOf(rids(byFilter));
    // The filter loads every record in scope (200 of them) to list their RIDs; the predicate only the candidates the search met
    assertThat(whereLoads).isLessThan(filterLoads);
  }

  @Test
  void aSqlConditionWithAParameterRestrictsTheCandidates() {
    seedDense();

    final List<Result> rows = query("""
        SELECT expand(`vector.neighbors`('Passage[embedding]', :q, 8, { where: 'space = :space AND year >= :minYear' }))""",
        Map.of("q", QUERY, "space", "b", "minYear", 2020));

    assertThat(rows).hasSize(8);
    for (final Result r : rows) {
      assertThat(r.<String>getProperty("space")).isEqualTo("b");
      assertThat(r.<Integer>getProperty("year")).isGreaterThanOrEqualTo(2020);
    }
    final List<Result> reference = query("""
        SELECT expand(`vector.neighbors`('Passage[embedding]', ?, 8,
          { filter: (SELECT @rid FROM Passage WHERE space = 'b' AND year >= 2020).@rid }))""", QUERY);
    assertThat(rids(rows)).containsExactlyElementsOf(rids(reference));
  }

  @Test
  void whereAndFilterTogetherKeepOnlyTheCandidatesBothAccept() {
    seedDense();
    final List<RID> scope = new ArrayList<>();
    for (final Result r : query("SELECT @rid AS rid FROM Passage WHERE year < 2005"))
      scope.add(r.getProperty("rid"));

    final List<Result> rows = query("""
        SELECT expand(`vector.neighbors`('Passage[embedding]', :q, 5, { filter: :scope, where: { space: 'a' } }))""",
        Map.of("q", QUERY, "scope", scope));

    assertThat(rows).hasSize(5);
    for (final Result r : rows) {
      assertThat(r.<String>getProperty("space")).isEqualTo("a");
      assertThat(r.<Integer>getProperty("year")).isLessThan(2005);
    }
  }

  @Test
  void aPredicateNoRecordSatisfiesAnswersNothing() {
    seedDense();

    assertThat(query("""
        SELECT expand(`vector.neighbors`('Passage[embedding]', ?, 5, { where: { space: 'none' } }))""", QUERY)).isEmpty();
  }

  @Test
  void aWhereThatIsNeitherAMapNorAConditionIsRefused() {
    seedDense();

    assertThatThrownBy(() -> query("SELECT expand(`vector.neighbors`('Passage[embedding]', ?, 5, { where: 3 }))", QUERY))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("'where'");
    assertThatThrownBy(() -> query("SELECT expand(`vector.neighbors`('Passage[embedding]', ?, 5, { where: {} }))", QUERY))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("'where'");
  }

  @Test
  void aPositionalParameterInAConditionIsRefused() {
    seedDense();

    // the `?` of the condition would be read as the query's first positional value, the query vector
    assertThatThrownBy(() -> query("SELECT expand(`vector.neighbors`('Passage[embedding]', ?, 5, { where: 'space = ?' }))", QUERY,
        "a")).isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("positional");
    assertThat(RecordPredicateRidSet.hasPositionalParameter("space = '?' AND `a?b` = \"x?\"")).isFalse();
    assertThat(RecordPredicateRidSet.hasPositionalParameter("space = 'it\\'s' AND year = ?")).isTrue();
    assertThat(RecordPredicateRidSet.hasPositionalParameter("title = 'it\\'s ?'")).isFalse();
  }

  @Test
  void sparseNeighborsTakesTheSameWhere() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Sparse BUCKETS 2");
      database.command("sql", "CREATE PROPERTY Sparse.space STRING");
      database.command("sql", "CREATE PROPERTY Sparse.tokens ARRAY_OF_INTEGERS");
      database.command("sql", "CREATE PROPERTY Sparse.weights ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Sparse (tokens, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 16 }");
    });
    final Random random = new Random(10169);
    database.transaction(() -> {
      for (int i = 0; i < 200; i++)
        database.newDocument("Sparse")
            .set("space", i % 2 == 0 ? "a" : "b")
            .set("tokens", new int[] { 0, 1 + random.nextInt(15) })
            .set("weights", new float[] { random.nextFloat(), random.nextFloat() })
            .save();
    });

    final List<Result> byWhere = query("""
        SELECT expand(`vector.sparseNeighbors`('Sparse[tokens,weights]', [0], [1.0], 6, { where: "space = 'b'" }))""");
    final List<Result> byFilter = query("""
        SELECT expand(`vector.sparseNeighbors`('Sparse[tokens,weights]', [0], [1.0], 6,
          { filter: (SELECT @rid FROM Sparse WHERE space = 'b').@rid }))""");

    assertThat(byWhere).hasSize(6);
    for (final Result r : byWhere)
      assertThat(r.<String>getProperty("space")).isEqualTo("b");
    assertThat(rids(byWhere)).containsExactlyElementsOf(rids(byFilter));
  }
}
