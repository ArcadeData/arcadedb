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
import com.arcadedb.index.MultiIndexCursor;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * https://github.com/ArcadeData/arcadedb/issues/6575
 * <p>
 * {@code SelectOperator.not} has no fluent entry point on the {@code Select} builder today (no {@code .not()} method
 * exists on {@code SelectWhereLeftBlock}/{@code SelectWhereOperatorBlock}/etc.), so the where-tree in these tests is
 * built by hand, the same technique already used by
 * {@code Issue6565SelectIndexCandidateLimitTest#notLeafIsNeverTreatedAsExactlyIndexed} and
 * {@code SelectCompositeIndexTest#compositeIndexNotUsedWithNotInTheConjunction} - neither of which happens to cover
 * this defect: the first only asserts the candidate cap stays {@code -1}, and the second returns a {@code null}
 * cursor only because neither of its two properties has a standalone index, not because the code excludes a leaf
 * under {@code not}.
 * <p>
 * {@code isTheNodeFullyIndexed()} walks through a {@code not} node the same way it walks through {@code and}/{@code
 * or}, setting {@code node.index} on the leaf underneath exactly as it would for a plain, un-negated leaf.
 * {@code filterWithIndexesFinalNode()} then had no special case for a {@code not} parent (only {@code or} and
 * {@code and} are handled), so for a where-tree shaped {@code NOT (a = 'x')} against a standalone index on
 * {@code a}, it built {@code node.index.get(new Object[]{"x"})} - a cursor of the records where {@code a = 'x'}
 * <b>is</b> true, the exact opposite of what {@code NOT (a = 'x')} should select. {@code evaluateWhere()} would then
 * reject every one of those candidates (they all satisfy the positive condition by construction), so the query
 * silently returned zero rows instead of "every record where {@code a != 'x'}".
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SelectNotWrappedIndexedLeafTest extends TestHelper {

  public SelectNotWrappedIndexedLeafTest() {
    autoStartTx = true;
  }

  @Override
  protected void beginTest() {
    final var t = database.getSchema().createDocumentType("T");
    t.createProperty("a", Type.STRING);
    t.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a");

    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        final var d = database.newDocument("T");
        d.set("a", i == 0 ? "x" : "y");
        d.save();
      }
    });
  }

  @Test
  void notWrappedIndexedEqLeafBuildsNoCursor() {
    // NOT (a = 'x'): 'a' HAS A STANDALONE INDEX, SO isTheNodeFullyIndexed() SETS node.index ON THE LEAF EXACTLY AS
    // IT WOULD FOR A PLAIN 'a = x'. filterWithIndexesFinalNode() MUST REFUSE TO BUILD A CURSOR FOR IT ANYWAY, SINCE
    // A CURSOR HERE WOULD YIELD THE RECORDS WHERE a = 'x' IS TRUE - THE OPPOSITE OF WHAT NOT (a = 'x') SELECTS.
    final Select select = database.select().fromType("T");
    final SelectTreeNode eqLeaf = new SelectTreeNode(new SelectPropertyValue("a"), SelectOperator.eq, "x");
    select.rootTreeElement = new SelectTreeNode(eqLeaf, SelectOperator.not, null);

    final SelectExecutor executor = new SelectExecutor(select);
    final MultiIndexCursor cursor = executor.lookForIndexes();
    try {
      // A null CURSOR MEANS execute() FALLS BACK TO A FULL TYPE SCAN, WHICH IS THE ONLY SAFE OPTION HERE - A CAP
      // WOULD REQUIRE THE (WRONG) POSITIVE CURSOR THIS TEST EXISTS TO REJECT.
      assertThat((Object) cursor).isNull();
      assertThat(executor.metrics().get("usedIndexes")).isEqualTo(0);
    } finally {
      if (cursor != null)
        cursor.close();
    }
  }

  @Test
  void notWrappedIndexedLeafUnderAndStillBuildsNoCursorForTheNegatedSide() {
    // NOT (a = 'x') AND a = 'y': THE and's LEFT CHILD IS A not NODE, NOT A BARE LEAF - filterWithIndexes() RECURSES
    // THROUGH IT AND filterWithIndexesFinalNode() MUST STILL REFUSE THE CURSOR FOR THE LEAF UNDERNEATH, WHILE THE
    // RIGHT (UN-NEGATED) LEAF IS FREE TO GET ITS OWN CURSOR AS USUAL.
    final Select select = database.select().fromType("T");
    final SelectTreeNode notEqXLeaf = new SelectTreeNode(new SelectPropertyValue("a"), SelectOperator.eq, "x");
    final SelectTreeNode notEqX = new SelectTreeNode(notEqXLeaf, SelectOperator.not, null);
    final SelectTreeNode eqYLeaf = new SelectTreeNode(new SelectPropertyValue("a"), SelectOperator.eq, "y");
    select.rootTreeElement = new SelectTreeNode(notEqX, SelectOperator.and, eqYLeaf);

    final SelectExecutor executor = new SelectExecutor(select);
    final MultiIndexCursor cursor = executor.lookForIndexes();
    try {
      // ONLY THE RIGHT (UN-NEGATED) LEAF CONTRIBUTED A CURSOR
      assertThat(executor.metrics().get("usedIndexes")).isEqualTo(1);
    } finally {
      if (cursor != null)
        cursor.close();
    }
  }
}
