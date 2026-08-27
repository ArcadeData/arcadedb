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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #6796: {@code WHERE indexedProperty NOT IN [...]} returned the same rows as {@code IN} - the negation was
 * silently lost. The index planner intercepted the {@code InCondition} on an indexed property and built a
 * cursor over the values that DO match, without ever consulting the condition's {@code not} flag; the
 * unindexed sibling property of the same rows answered correctly, which is what made it look index-dependent.
 * <p>
 * A negated leaf has no complement in {@link com.arcadedb.query.sql.executor.FetchFromIndexStep}, so the only
 * correct answer is for {@code isIndexAware()} to decline whenever {@code not} is set - for every right-hand
 * shape - and let the full-scan evaluator apply the negation, which it has always done correctly.
 * <p>
 * This test locks the reporter's shape down end to end: a STRING property under a UNIQUE index, both a value
 * that matches exactly one row and a value that matches nothing, and the sibling unindexed property that has
 * to keep agreeing with it. It complements {@link Issue6640ParenthesizedInListIndexTest}, which covers the
 * numeric/bound-parameter forms, and extends the guard to the shapes that class never reached: NOT IN under
 * an AND with a second indexed predicate, under an OR, against a subquery, and against a composite index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6796NotInIndexedPropertyTest extends TestHelper {

  private static final int    ROWS      = 500;
  private static final String PRESENT   = "d_eb202a039f14c8bb";
  private static final String ABSENT    = "value_matching_nothing";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final com.arcadedb.schema.VertexType type = database.getSchema().createVertexType("Document");
      type.createProperty("doc_id", Type.STRING);
      type.createProperty("ocr_status", Type.STRING);
      type.createProperty("tenant", Type.INTEGER);
      database.getSchema().buildTypeIndex("Document", new String[] { "doc_id" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();
      database.getSchema().buildTypeIndex("Document", new String[] { "tenant" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();
    });

    database.transaction(() -> {
      // Row 0 carries the reporter's exact doc_id so the "matches one row" case is deterministic.
      database.newVertex("Document").set("doc_id", PRESENT).set("ocr_status", PRESENT).set("tenant", 1).save();
      for (int i = 1; i < ROWS; i++)
        database.newVertex("Document").set("doc_id", "d_" + i).set("ocr_status", "d_" + i).set("tenant", i % 5).save();
    });
  }

  @Test
  void notInOnUniqueIndexedStringExcludesTheListedRow() {
    database.transaction(() -> {
      assertThat(count("select from Document where doc_id in ['" + PRESENT + "']")).isEqualTo(1);
      assertThat(count("select from Document where doc_id not in ['" + PRESENT + "']")).isEqualTo(ROWS - 1);
      assertThat(count("select from Document where doc_id not in ['" + ABSENT + "']")).isEqualTo(ROWS);
    });
  }

  @Test
  void notInAgreesWithTheUnindexedSiblingProperty() {
    database.transaction(() -> {
      // ocr_status holds exactly the same values as doc_id but carries no index: the two must never disagree.
      assertThat(count("select from Document where ocr_status not in ['" + PRESENT + "']"))
          .isEqualTo(count("select from Document where doc_id not in ['" + PRESENT + "']"));
      assertThat(count("select from Document where ocr_status not in ['" + ABSENT + "']"))
          .isEqualTo(count("select from Document where doc_id not in ['" + ABSENT + "']"));
    });
  }

  @Test
  void notInAgreesWithTheEquivalentRewrites() {
    database.transaction(() -> {
      final long expected = ROWS - 1;
      assertThat(count("select from Document where doc_id not in ['" + PRESENT + "']")).isEqualTo(expected);
      assertThat(count("select from Document where not (doc_id in ['" + PRESENT + "'])")).isEqualTo(expected);
      assertThat(count("select from Document where doc_id <> '" + PRESENT + "'")).isEqualTo(expected);
    });
  }

  @Test
  void notInDeclinesTheIndexForEveryRightHandShape() {
    database.transaction(() -> {
      assertThat(planOf("select from Document where doc_id not in ['" + PRESENT + "']")).doesNotContain("FETCH FROM INDEX");
      assertThat(planOf("select from Document where doc_id not in ('" + PRESENT + "')")).doesNotContain("FETCH FROM INDEX");
      final ResultSet explain = database.query("sql", "explain select from Document where doc_id not in :ids",
          Map.of("ids", List.of(PRESENT)));
      assertThat(explain.getExecutionPlan().get().prettyPrint(0, 2)).doesNotContain("FETCH FROM INDEX");
    });
  }

  @Test
  void notInStaysNegatedWhenAnotherIndexedPredicateDrivesTheFetch() {
    database.transaction(() -> {
      // tenant is indexed, so the planner is free to drive the fetch from that index; the NOT IN left over as
      // a residual filter must still exclude, not include. tenant=1 covers rows 1, 6, 11, ... plus row 0.
      final long tenant1 = count("select from Document where tenant = 1");
      assertThat(count("select from Document where tenant = 1 and doc_id not in ['" + PRESENT + "']")).isEqualTo(tenant1 - 1);
      assertThat(count("select from Document where tenant = 1 and doc_id in ['" + PRESENT + "']")).isEqualTo(1);
    });
  }

  @Test
  void notInStaysNegatedUnderOr() {
    database.transaction(() -> {
      // Every row except the one whose doc_id is excluded, plus that row back again through the tenant leg.
      assertThat(count("select from Document where doc_id not in ['" + PRESENT + "'] or tenant = 1")).isEqualTo(ROWS);
      assertThat(count("select from Document where doc_id not in ['" + PRESENT + "'] or tenant = 99")).isEqualTo(ROWS - 1);
    });
  }

  @Test
  void notInAgainstASubqueryExcludesTheMatchedRows() {
    database.transaction(() -> {
      assertThat(count("select from Document where doc_id not in (select doc_id from Document where tenant = 1)"))
          .isEqualTo(ROWS - count("select from Document where tenant = 1"));
    });
  }

  @Test
  void notInOnACompositeIndexLeadingColumnExcludesTheListedRows() {
    database.transaction(() -> database.getSchema().buildTypeIndex("Document", new String[] { "tenant", "doc_id" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create());

    database.transaction(() -> {
      final long tenant0 = count("select from Document where tenant = 0");
      assertThat(count("select from Document where tenant not in [0]")).isEqualTo(ROWS - tenant0);
      assertThat(planOf("select from Document where tenant not in [0]")).doesNotContain("FETCH FROM INDEX");
    });
  }

  @Test
  void notInWithANullInTheListExcludesEveryRow() {
    database.transaction(() -> {
      // SQL three-valued logic: a miss against a list holding NULL is UNKNOWN, not FALSE, so NOT IN keeps
      // nothing. The memoized probe records the null element separately for exactly this reason.
      assertThat(count("select from Document where doc_id not in ['" + ABSENT + "', null]")).isZero();
      assertThat(count("select from Document where doc_id in ['" + PRESENT + "', null]")).isEqualTo(1);
    });
  }

  @Test
  void aRightHandSideThatIsNotConstantIsStillEvaluatedPerRow() {
    database.transaction(() -> {
      // ocr_status is a property, so the list is a different value on every row and must not be memoized:
      // every row matches itself, so IN keeps all of them and NOT IN keeps none.
      assertThat(count("select from Document where doc_id in [ocr_status]")).isEqualTo(ROWS);
      assertThat(count("select from Document where doc_id not in [ocr_status]")).isZero();
    });
  }

  @Test
  void aBoundParameterListIsMemoizedWithoutLosingTheNegation() {
    database.transaction(() -> {
      assertThat(database.query("sql", "select from Document where doc_id not in :ids", Map.of("ids", List.of(PRESENT)))
          .stream().count()).isEqualTo(ROWS - 1);
      assertThat(database.query("sql", "select from Document where doc_id in :ids", Map.of("ids", List.of(PRESENT)))
          .stream().count()).isEqualTo(1);
    });
  }

  @Test
  @Tag("slow")
  void notInCostDoesNotGrowWithTheSizeOfAConstantList() {
    final int extraRows = 20_000;
    database.transaction(() -> {
      for (int i = 0; i < extraRows; i++)
        database.newVertex("Document").set("doc_id", "bulk_" + i).set("tenant", i % 5).save();
    });

    // NOT IN has no index cursor to use, so the whole type is scanned with the condition as a residual filter.
    // Before the constant right-hand side was hoisted and hashed, that filter rebuilt the entire list from the
    // parse tree and rescanned it linearly for every row: 20,000 rows against a 2,000-value list took ~1s and
    // grew linearly in both factors, which put the reported 15,000-value NOT IN over a 36M-row type in the hours.
    database.transaction(() -> assertThat(count(notInQuery(200))).isEqualTo(ROWS + extraRows));

    // assertStayedUnder, not assertGaveUpWithin: the bound IS the assertion, standing in for "the per-row cost
    // no longer carries the list". Widening it would not be a safe loosening, it would delete the coverage.
    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    database.transaction(() -> assertThat(count(notInQuery(20_000))).isEqualTo(ROWS + extraRows));
    stopwatch.assertStayedUnder(5_000,
        "a 20,000-value NOT IN over 20,500 rows paying for the list once, not once per row");
  }

  private String notInQuery(final int listSize) {
    final StringBuilder sql = new StringBuilder("select from Document where doc_id not in [");
    for (int i = 0; i < listSize; i++) {
      if (i > 0)
        sql.append(',');
      sql.append("'missing_").append(i).append('\'');
    }
    return sql.append(']').toString();
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      return rs.stream().count();
    }
  }

  private String planOf(final String query) {
    try (final ResultSet rs = database.query("sql", "explain " + query)) {
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }
}
