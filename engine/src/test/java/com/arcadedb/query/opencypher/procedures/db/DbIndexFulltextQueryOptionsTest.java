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
package com.arcadedb.query.opencypher.procedures.db;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.opencypher.procedures.CypherProcedureRegistry;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8103: Neo4j declares {@code db.index.fulltext.queryNodes(indexName, queryString, options = {})} - and the
 * same shape for {@code queryRelationships} - while ArcadeDB accepted exactly two arguments, so a call carrying
 * {@code options} was rejected with {@code expects 2 arguments but got 3} rather than being honoured.
 * <p>
 * {@code skip} and {@code limit} are honoured; every other key Neo4j accepts, {@code analyzer} included, is
 * rejected by name rather than silently ignored - swallowing an option the caller expects to take effect is worse
 * than refusing the call.
 */
class DbIndexFulltextQueryOptionsTest {
  private Database database;

  /**
   * Four articles with the same token count - so BM25's document-length normalization is identical - and a
   * strictly decreasing "java" term frequency, which makes the ranking A &gt; B &gt; C &gt; D deterministic and
   * every skip/limit assertion below an assertion about position, not about a tie-break.
   */
  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/fulltext-query-options-8103").create();
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "CREATE EDGE TYPE Cites");
      database.command("sql", "CREATE PROPERTY Cites.note STRING");
      database.command("sql", "CREATE INDEX ON Cites (note) FULL_TEXT");
    });

    database.transaction(() -> {
      database.newVertex("Article").set("title", "A").set("content", "java java java java alpha").save();
      database.newVertex("Article").set("title", "B").set("content", "java java java alpha beta").save();
      database.newVertex("Article").set("title", "C").set("content", "java java alpha beta gamma").save();
      database.newVertex("Article").set("title", "D").set("content", "java alpha beta gamma delta").save();
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /** Without options the three-argument form has to behave exactly like the two-argument one. */
  @Test
  void anEmptyOptionsMapBehavesLikeTheTwoArgumentCall() {
    assertThat(titles("{}")).isEqualTo(titles(null));
    assertThat(titles(null)).containsExactly("A", "B", "C", "D");
  }

  /** An explicit {@code null} in the options slot is the same as omitting it. */
  @Test
  void aNullOptionsMapBehavesLikeTheTwoArgumentCall() {
    assertThat(titles("null")).containsExactly("A", "B", "C", "D");
  }

  @Test
  void limitKeepsOnlyTheHighestScoringRows() {
    assertThat(titles("{limit: 2}")).containsExactly("A", "B");
  }

  @Test
  void skipDropsTheHighestScoringRows() {
    assertThat(titles("{skip: 2}")).containsExactly("C", "D");
  }

  @Test
  void skipAndLimitTogetherPaginate() {
    assertThat(titles("{skip: 1, limit: 2}")).containsExactly("B", "C");
    assertThat(titles("{skip: 3, limit: 2}")).as("the last page is short, not padded").containsExactly("D");
  }

  @Test
  void aSkipPastTheEndOfTheResultSetReturnsNoRows() {
    assertThat(titles("{skip: 10}")).isEmpty();
  }

  @Test
  void aLimitOfZeroReturnsNoRows() {
    assertThat(titles("{limit: 0}")).isEmpty();
  }

  @Test
  void aLimitLargerThanTheResultSetReturnsEverything() {
    assertThat(titles("{limit: 100}")).containsExactly("A", "B", "C", "D");
  }

  /**
   * The point of rejecting rather than ignoring: {@code analyzer} is a Neo4j key ArcadeDB cannot honour, because
   * the analyzer is resolved from the index metadata written at index-creation time. Accepting the call and
   * ignoring the key would run the query under an analyzer the caller did not ask for and report success.
   */
  @Test
  void theAnalyzerOptionIsRejectedByName() {
    assertThatThrownBy(() -> titles("{analyzer: 'whitespace'}"))
        .hasStackTraceContaining("analyzer");
  }

  @Test
  void anUnknownOptionKeyIsRejectedByName() {
    assertThatThrownBy(() -> titles("{nosuchoption: 1}"))
        .hasStackTraceContaining("nosuchoption");
  }

  @Test
  void aNegativeSkipOrLimitIsRejected() {
    assertThatThrownBy(() -> titles("{skip: -1}")).hasStackTraceContaining("skip");
    assertThatThrownBy(() -> titles("{limit: -1}")).hasStackTraceContaining("limit");
  }

  @Test
  void aNonIntegerSkipOrLimitIsRejected() {
    assertThatThrownBy(() -> titles("{limit: 'two'}")).hasStackTraceContaining("limit");
    assertThatThrownBy(() -> titles("{skip: 1.5}")).hasStackTraceContaining("skip");
  }

  /**
   * A whole number above 2^53 is still a whole number: the integrality check must not mistake the precision a
   * {@code Long} loses on its way through {@code double} for a fractional value the caller wrote.
   */
  @Test
  void aVeryLargeWholeLimitIsClampedRatherThanRejected() {
    assertThat(titles("{limit: 9007199254740993}")).containsExactly("A", "B", "C", "D");
    assertThat(titles("{skip: 9007199254740993}")).isEmpty();
  }

  @Test
  void aNonMapOptionsArgumentIsRejected() {
    assertThatThrownBy(() -> titles("'not-a-map'")).hasStackTraceContaining("options");
  }

  /** The relationship counterpart takes the very same options. */
  @Test
  void queryRelationshipsHonoursSkipAndLimit() {
    database.transaction(() -> {
      database.command("sql", "CREATE EDGE Cites FROM (SELECT FROM Article WHERE title = 'A') "
          + "TO (SELECT FROM Article WHERE title = 'B') SET note = 'java java java java alpha'");
      database.command("sql", "CREATE EDGE Cites FROM (SELECT FROM Article WHERE title = 'B') "
          + "TO (SELECT FROM Article WHERE title = 'C') SET note = 'java java java alpha beta'");
      database.command("sql", "CREATE EDGE Cites FROM (SELECT FROM Article WHERE title = 'C') "
          + "TO (SELECT FROM Article WHERE title = 'D') SET note = 'java java alpha beta gamma'");
    });

    assertThat(notes(null)).hasSize(3);
    assertThat(notes("{limit: 1}")).containsExactly("java java java java alpha");
    assertThat(notes("{skip: 1, limit: 1}")).containsExactly("java java java alpha beta");
    assertThat(notes("{skip: 3}")).isEmpty();
  }

  @Test
  void queryRelationshipsRejectsAnUnsupportedOptionKey() {
    assertThatThrownBy(() -> notes("{analyzer: 'whitespace'}")).hasStackTraceContaining("analyzer");
  }

  /**
   * The direct-caller entry point for both procedures: {@code execute()} is public, so the gate every caller
   * passes through is {@code validateArgs}. It has to accept 2 and 3 arguments and keep rejecting 1 and 4.
   */
  @Test
  void arityGateAcceptsTwoOrThreeArgumentsAndStillRejectsTheRest() {
    for (final String name : List.of("db.index.fulltext.queryNodes", "db.index.fulltext.queryRelationships")) {
      final CypherProcedure procedure = CypherProcedureRegistry.get(name);

      assertThat(procedure.getMinArgs()).as("%s min", name).isEqualTo(2);
      assertThat(procedure.getMaxArgs()).as("%s max", name).isEqualTo(3);

      assertThatCode(() -> procedure.validateArgs(new Object[2])).doesNotThrowAnyException();
      assertThatCode(() -> procedure.validateArgs(new Object[3])).doesNotThrowAnyException();

      assertThatThrownBy(() -> procedure.validateArgs(new Object[1]))
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("expects 2-3 arguments but got 1");
      assertThatThrownBy(() -> procedure.validateArgs(new Object[4]))
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("expects 2-3 arguments but got 4");
    }
  }

  private List<String> titles(final String options) {
    return collect("CALL db.index.fulltext.queryNodes('Article[content]', 'java'" + argument(options) + ") "
        + "YIELD node, score RETURN node.title AS v", "v");
  }

  private List<String> notes(final String options) {
    return collect("CALL db.index.fulltext.queryRelationships('Cites[note]', 'java'" + argument(options) + ") "
        + "YIELD relationship, score RETURN relationship.note AS v", "v");
  }

  private static String argument(final String options) {
    return options == null ? "" : ", " + options;
  }

  private List<String> collect(final String query, final String field) {
    final List<String> values = new ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", query)) {
      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        values.add(row.getProperty(field));
      }
    }
    return values;
  }
}
