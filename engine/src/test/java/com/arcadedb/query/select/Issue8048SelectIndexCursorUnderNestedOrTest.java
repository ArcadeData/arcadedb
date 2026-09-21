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
package com.arcadedb.query.select;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * https://github.com/ArcadeData/arcadedb/issues/8048
 * <p>
 * The union of the cursors {@code SelectExecutor.filterWithIndexesFinalNode()} builds is the ONLY set of candidate
 * records a native select ever evaluates, so a cursor is sound only when every record that could satisfy the WHERE
 * is guaranteed to be inside it. Under an {@code or} that means both sides have to be index-answerable, and the
 * method did check that - but only against the leaf's IMMEDIATE sibling. Two ways out of it, both reproduced here:
 * <ol>
 *   <li>A leaf nested one level deeper. In {@code b = 1 and a = 0 or b = 2} the tree is
 *   {@code or(and(b=1, a=0), b=2)}, the indexed leaf {@code a = 0} has the {@code and} as its parent, the
 *   {@code parent.operator == or} test was false and the cursor was built unconditionally. The candidate set
 *   collapsed to "records with a = 0" and every record matching {@code b = 2} was never looked at.</li>
 *   <li>A composite {@code or} sibling. {@code isTheNodeFullyIndexed()} answered {@code left || right} for an
 *   {@code or} node, where an OR branch is only index-answerable when BOTH of its sides are.</li>
 * </ol>
 * The invariant under test is the one this whole code path exists to preserve: an index is a PERFORMANCE decision,
 * so the same select over the same data must return the same rows with and without one. Each case is therefore run
 * twice against the identical SQL - once with an {@code LSM_TREE} index on {@code a} and once with none - and the
 * un-indexed run is the control that was always correct.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue8048SelectIndexCursorUnderNestedOrTest extends TestHelper {

  private static final int CARDINALITY = 6;

  @Override
  protected void beginTest() {
    final DocumentType type = database.getSchema().createDocumentType("D");
    type.createProperty("a", Type.INTEGER);
    type.createProperty("b", Type.INTEGER);

    database.transaction(() -> {
      for (int a = 0; a < CARDINALITY; a++)
        for (int b = 0; b < CARDINALITY; b++) {
          final MutableDocument doc = database.newDocument("D");
          doc.set("a", a);
          doc.set("b", b);
          doc.save();
        }
    });
  }

  @Test
  void indexedLeafNestedUnderAnAndInsideAnOrDoesNotDropTheOtherBranch() {
    // b = 1 and a = 0 or b = 2 -> or(and(b=1, a=0), b=2)
    runWithAndWithoutIndex(() -> database.select().fromType("D").where()//
        .property("b").eq().value(1)//
        .and().property("a").eq().value(0)//
        .or().property("b").eq().value(2).count(), "b = 1 and a = 0 or b = 2");
  }

  @Test
  void indexedLeafFirstInsideTheAndBranchDoesNotDropTheOtherBranch() {
    runWithAndWithoutIndex(() -> database.select().fromType("D").where()//
        .property("a").eq().value(0)//
        .and().property("b").eq().value(1)//
        .or().property("b").eq().value(2).count(), "a = 0 and b = 1 or b = 2");
  }

  @Test
  void indexedLeafInTheAndBranchOnTheRightOfTheOrDoesNotDropTheOtherBranch() {
    runWithAndWithoutIndex(() -> database.select().fromType("D").where()//
        .property("b").eq().value(2)//
        .or().property("a").eq().value(0)//
        .and().property("b").eq().value(1).count(), "b = 2 or a = 0 and b = 1");
  }

  @Test
  void chainedOrsWithAnUnindexedTailDoNotDropIt() {
    // a = 1 or a = 3 or b = 2: THE LEFT-NESTED or MEANS THE FIRST TWO LEAVES' IMMEDIATE SIBLING IS INDEXED, WHILE
    // THE OUTER or'S OTHER BRANCH - b = 2 - IS NOT. ONLY A PATH WALK SEES THAT
    runWithAndWithoutIndex(() -> database.select().fromType("D").where()//
        .property("a").eq().value(1)//
        .or().property("a").eq().value(3)//
        .or().property("b").eq().value(2).count(), "a = 1 or a = 3 or b = 2");
  }

  @Test
  void rightNestedOrSiblingIsOnlyIndexedWhenBothOfItsSidesAre() {
    // a = 1 OR (b = 1 OR a = 2). THE FLUENT BUILDER CANNOT PRODUCE A RIGHT-NESTED or, BUT Select.json() CAN.
    // isTheNodeFullyIndexed()'S or ARM USED TO ANSWER left || right, SO THE INNER or CALLED ITSELF FULLY INDEXED ON
    // THE STRENGTH OF a = 2 ALONE AND THE OUTER a = 1 LEAF GOT ITS CURSOR
    final JSONObject json = new JSONObject(
        "{\"fromType\":\"D\",\"where\":[[\":a\",\"=\",1],\"or\",[[\":b\",\"=\",1],\"or\",[\":a\",\"=\",2]]]}");

    runWithAndWithoutIndex(() -> database.select().json(json).count(), "a = 1 or (b = 1 or a = 2)");
  }

  @Test
  void plainOrWithOneIndexedSideStaysCorrect() {
    // THE CONTROL FROM THE ISSUE: HERE THE LEAF'S PARENT *IS* THE or, THE PRE-EXISTING GATE ALREADY FIRED AND THE
    // ANSWER WAS ALWAYS RIGHT. IT MUST STAY RIGHT
    runWithAndWithoutIndex(() -> database.select().fromType("D").where()//
        .property("a").eq().value(0)//
        .or().property("b").eq().value(2).count(), "a = 0 or b = 2");
  }

  @Test
  void anIndexIsStillUsedWhenBothOrBranchesAreIndexed() {
    // THE FIX MUST NOT DISABLE THE INDEX PATH ALTOGETHER: WITH BOTH PROPERTIES INDEXED, BOTH BRANCHES ARE
    // ANSWERABLE AND TWO CURSORS MUST STILL BE BUILT
    database.getSchema().getType("D").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a");
    database.getSchema().getType("D").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "b");
    try {
      final Select select = database.select().fromType("D");
      select.where().property("a").eq().value(0).or().property("b").eq().value(2).compile();

      final SelectExecutor executor = new SelectExecutor(select);
      try (var cursor = executor.lookForIndexes()) {
        assertThat((Object) cursor).isNotNull();
        assertThat(executor.metrics().get("usedIndexes")).isEqualTo(2);
      }

      assertThat(database.select().fromType("D").where()//
          .property("a").eq().value(0)//
          .or().property("b").eq().value(2).count()).isEqualTo(sqlCount("a = 0 or b = 2"));
    } finally {
      dropIndexesOn("a");
      dropIndexesOn("b");
    }
  }

  /**
   * Runs {@code nativeCount} against the identical SQL twice: once with an index on {@code a} and once without.
   * Both answers have to match SQL, which also makes them match each other - the invariant an index must never
   * break.
   */
  private void runWithAndWithoutIndex(final java.util.function.LongSupplier nativeCount, final String sqlWhere) {
    final long expected = sqlCount(sqlWhere);

    assertThat(nativeCount.getAsLong()).as("without an index on 'a': " + sqlWhere).isEqualTo(expected);

    database.getSchema().getType("D").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a");
    try {
      assertThat(nativeCount.getAsLong()).as("with an index on 'a': " + sqlWhere).isEqualTo(expected);
    } finally {
      dropIndexesOn("a");
    }
  }

  private void dropIndexesOn(final String property) {
    for (final var index : database.getSchema().getType("D").getIndexesByProperties(property))
      database.getSchema().dropIndex(index.getName());
  }

  private long sqlCount(final String where) {
    return ((Number) database.query("sql", "SELECT count(*) AS c FROM D WHERE " + where).nextIfAvailable().getProperty("c"))
        .longValue();
  }
}
