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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.MultiTypeIndexes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11014: the logical name of a Cypher {@code CREATE FULLTEXT INDEX} / {@code CREATE VECTOR INDEX} over several labels is
 * resolved by SQL too: {@code SEARCH_INDEX} and {@code vector.neighbors} fan out to the per-type indexes, {@code DROP INDEX} of the
 * name drops what the statement built, and {@code DROP INDEX} of a member is refused.
 */
class Issue11014SqlResolvesMultiTypeIndexNameTest extends TestHelper {

  private static final String FULLTEXT_DDL = "CREATE FULLTEXT INDEX docs FOR (n:Article|Post) ON EACH [n.title, n.body]";
  private static final String VECTOR_DDL   = "CREATE VECTOR INDEX emb FOR (n:Article|Post) ON n.e OPTIONS {indexConfig: "
      + "{`vector.dimensions`: 2, `vector.similarity_function`: 'cosine'}}";

  @BeforeEach
  void createIndexes() {
    // a common supertype, so one SQL statement can target the types the index spans
    sql("CREATE VERTEX TYPE Doc");
    sql("CREATE VERTEX TYPE Article EXTENDS Doc");
    sql("CREATE VERTEX TYPE Post EXTENDS Doc");
    cypher(FULLTEXT_DDL);
    cypher(VECTOR_DDL);
    cypher("CREATE (:Article {id: 'a1', title: 'graph databases', body: 'fast traversal', e: [1.0, 0.0]})");
    cypher("CREATE (:Post {id: 'p1', title: 'cooking', body: 'a graph of recipes', e: [0.9, 0.1]})");
    cypher("CREATE (:Post {id: 'p2', title: 'travel', body: 'mountains', e: [0.0, 1.0]})");
  }

  @Test
  void searchIndexByTheLogicalNameFindsEveryType() {
    assertThat(sqlIds("SELECT id FROM Doc WHERE SEARCH_INDEX('docs', 'graph') = true")).containsExactlyInAnyOrder("a1", "p1");
    assertThat(sqlIds("SELECT id FROM Doc WHERE SEARCH_INDEX('docs', 'mountains') = true")).containsExactly("p2");
  }

  @Test
  void searchIndexByTheLogicalNameIsRestrictedByTheTargetType() {
    // the member index of Post is not a candidate for Article rows: the per-row evaluation answers from the whole logical index
    assertThat(sqlIds("SELECT id FROM Article WHERE SEARCH_INDEX('docs', 'graph') = true")).containsExactly("a1");
    assertThat(sqlIds("SELECT id FROM Post WHERE SEARCH_INDEX('docs', 'graph') = true")).containsExactly("p1");
  }

  @Test
  void searchIndexByTheLogicalNameHonorsTheScoreOrderAndTheLimit() {
    assertThat(sqlIds("SELECT id FROM Doc WHERE SEARCH_INDEX('docs', 'graph') = true ORDER BY $score DESC LIMIT 2"))
        .containsExactlyInAnyOrder("a1", "p1");
    assertThat(sqlIds("SELECT id FROM Doc WHERE SEARCH_INDEX('docs', 'graph') = true ORDER BY $score DESC LIMIT 1")).hasSize(1);
    assertThat(sqlIds("SELECT id FROM Doc WHERE SEARCH_INDEX('docs', 'graph') = true LIMIT 0")).isEmpty();
  }

  @Test
  void searchIndexByTheLogicalNameIsExplained() {
    try (final ResultSet rs = database.query("sql", "EXPLAIN SELECT id FROM Doc WHERE SEARCH_INDEX('docs', 'graph') = true")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("executionPlanAsString")).contains("docs");
    }
  }

  @Test
  void recordsWrittenThroughSqlAreFoundByTheLogicalName() {
    database.transaction(() -> database.command("sql", "INSERT INTO Post SET id = 'p3', title = 'trip', body = 'a graph of roads'").close());
    assertThat(sqlIds("SELECT id FROM Doc WHERE SEARCH_INDEX('docs', 'graph') = true")).containsExactlyInAnyOrder("a1", "p1", "p3");
  }

  @Test
  void searchIndexByAnUnknownNameStillFails() {
    assertThatThrownBy(() -> sqlIds("SELECT id FROM Doc WHERE SEARCH_INDEX('nope', 'graph') = true")).isInstanceOf(SchemaException.class);
  }

  @Test
  void searchIndexByAVectorLogicalNameIsRefused() {
    assertThatThrownBy(() -> sqlIds("SELECT id FROM Doc WHERE SEARCH_INDEX('emb', 'graph') = true")).hasMessageContaining("not a full-text");
  }

  @Test
  void vectorNeighborsByTheLogicalNameMergesTheNearestOfEveryType() {
    assertThat(sqlIds("SELECT id FROM (SELECT expand(vector.neighbors('emb', [1.0, 0.0], 2)))")).containsExactly("a1", "p1");
    assertThat(sqlIds("SELECT id FROM (SELECT expand(vector.neighbors('emb', [0.0, 1.0], 1)))")).containsExactly("p2");

    database.transaction(() -> database.command("sql", "INSERT INTO Article SET id = 'a2', e = [0.3, 0.9]").close());
    assertThat(sqlIds("SELECT id FROM (SELECT expand(vector.neighbors('emb', [0.0, 1.0], 2)))")).containsExactly("p2", "a2");
  }

  @Test
  void vectorNeighborsByTheLogicalNameHonorsAnEmptyFilter() {
    assertThat(sqlIds("SELECT id FROM (SELECT expand(vector.neighbors('emb', [1.0, 0.0], 2, {filter: []})))")).isEmpty();
  }

  @Test
  void vectorNeighborsByAFullTextLogicalNameIsRefused() {
    assertThatThrownBy(() -> sqlIds("SELECT id FROM (SELECT expand(vector.neighbors('docs', [1.0, 0.0], 2)))"))
        .hasMessageContaining("not a vector index");
  }

  @Test
  void dropIndexOfAMemberIsRefusedAndLeavesTheLogicalIndexWhole() {
    assertThatThrownBy(() -> sql("DROP INDEX `Post[title,body]`")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("member of the multi-type index 'docs'");
    assertThatThrownBy(() -> sql("DROP INDEX `Post[title,body]` IF EXISTS")).isInstanceOf(SchemaException.class);

    assertThat(database.getSchema().existsIndex("Post[title,body]")).isTrue();
    assertThat(sqlIds("SELECT id FROM Doc WHERE SEARCH_INDEX('docs', 'graph') = true")).containsExactlyInAnyOrder("a1", "p1");
  }

  @Test
  void dropIndexOfAMemberIsRefusedFromCypherToo() {
    assertThatThrownBy(() -> cypher("DROP INDEX `Post[title,body]`")).hasRootCauseInstanceOf(SchemaException.class)
        .rootCause().hasMessageContaining("member of the multi-type index 'docs'");
    assertThat(database.getSchema().existsIndex("Post[title,body]")).isTrue();
  }

  @Test
  void dropIndexOfTheLogicalNameDropsWhatItsStatementBuilt() {
    sql("DROP INDEX docs");

    assertThat(MultiTypeIndexes.find(database.getSchema(), "docs")).isNull();
    assertThat(database.getSchema().existsIndex("Article[title,body]")).isFalse();
    assertThat(database.getSchema().existsIndex("Post[title,body]")).isFalse();
    // the other logical index is untouched
    assertThat(sqlIds("SELECT id FROM (SELECT expand(vector.neighbors('emb', [1.0, 0.0], 1)))")).containsExactly("a1");
    assertThatThrownBy(() -> sqlIds("SELECT id FROM Doc WHERE SEARCH_INDEX('docs', 'graph') = true")).isInstanceOf(SchemaException.class);
  }

  @Test
  void dropIndexOfTheLogicalNameKeepsAPerTypeIndexThatWasThereBefore() {
    cypher("DROP INDEX docs");
    cypher("CREATE FULLTEXT INDEX pre FOR (n:Article) ON EACH [n.title, n.body]");
    cypher(FULLTEXT_DDL.replace("docs FOR", "docs IF NOT EXISTS FOR"));
    assertThat(MultiTypeIndexes.find(database.getSchema(), "docs")).isNotNull();

    sql("DROP INDEX docs");

    // the Article member was there before `docs`, so it stays; the Post one was created by `docs`
    assertThat(database.getSchema().existsIndex("pre")).isTrue();
    assertThat(database.getSchema().existsIndex("Post[title,body]")).isFalse();
  }

  @Test
  void dropAllIndexesForgetsTheLogicalNames() {
    sql("DROP INDEX *");

    assertThat(MultiTypeIndexes.entries(database.getSchema())).isEmpty();
    assertThat(database.getSchema().getIndexes()).isEmpty();
  }

  @Test
  void createIndexCannotTakeTheLogicalName() {
    assertThatThrownBy(() -> {
      sql("CREATE PROPERTY Article.id STRING");
      sql("CREATE INDEX docs IF NOT EXISTS ON Article (id) NOTUNIQUE");
    }).isInstanceOf(AlreadyExistsException.class);
  }

  @Test
  void aPlainIndexIsStillDroppedByName() {
    sql("CREATE PROPERTY Article.id STRING");
    sql("CREATE INDEX ON Article (id) NOTUNIQUE");
    sql("DROP INDEX `Article[id]`");
    assertThat(database.getSchema().existsIndex("Article[id]")).isFalse();
  }

  private List<String> sqlIds(final String sql) {
    final List<String> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        ids.add(rs.next().getProperty("id"));
    }
    return ids;
  }

  private void sql(final String sql) {
    try (final ResultSet rs = database.command("sql", sql)) {
      while (rs.hasNext())
        rs.next();
    }
  }

  private void cypher(final String cypher) {
    try (final ResultSet rs = database.command("opencypher", cypher)) {
      while (rs.hasNext())
        rs.next();
    }
  }
}
