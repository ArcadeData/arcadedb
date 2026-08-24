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
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * https://github.com/ArcadeData/arcadedb/issues/6577
 * <p>
 * {@code SelectExecutor.isTheNodeFullyIndexed()} (used by {@code filterWithIndexesFinalNode()}'s "under an OR, both
 * sides must be indexed" gate) only excluded {@code is_null}/{@code is_not_null} from "this leaf is indexed" -
 * every other operator on an indexed property, including {@code neq}/{@code like}/{@code ilike}, was treated as
 * indexed as long as the property itself had an index. But {@code filterWithIndexesFinalNode()}'s cursor-building
 * switch only knows how to build a cursor for {@code eq}/{@code in_op}/{@code between}/{@code gt}/{@code ge}/
 * {@code lt}/{@code le} - a {@code neq}/{@code like}/{@code ilike} leaf silently contributed zero cursors while
 * still passing the "both sides indexed" gate for its {@code or} sibling. The result: for
 * {@code a != 'x' OR n = -1}, the {@code eq} leaf's cursor alone became the WHOLE candidate stream, and every
 * record matching only the {@code neq} branch was silently dropped - not "evaluated less efficiently", genuinely
 * missing from the result.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue6577OrWithNonCursorBuildableOperatorTest extends TestHelper {

  private static final int ROWS = 1000;

  public Issue6577OrWithNonCursorBuildableOperatorTest() {
    autoStartTx = true;
  }

  @Override
  protected void beginTest() {
    final var t = database.getSchema().createDocumentType("T");
    t.createProperty("a", Type.STRING);
    t.createProperty("n", Type.INTEGER);
    t.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a");
    t.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "n");

    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newDocument("T").set("a", "x", "n", i).save();
    });
  }

  @Test
  void orWithNeqOnOneSideMustNotDropTheOtherBranch() {
    // EVERY ROW HAS a = "x" != "nonexistent", SO THE neq BRANCH ALONE ALREADY MATCHES ALL ROWS
    final long count = database.select().fromType("T").where()//
        .property("a").neq().value("nonexistent")//
        .or().property("n").eq().value(-1).count();

    assertThat(count).isEqualTo(ROWS);
  }

  @Test
  void orWithNeqOnOneSideMustNotDropTheOtherBranchReversed() {
    // SAME SHAPE, neq ON THE RIGHT-HAND SIDE OF THE or INSTEAD OF THE LEFT
    final long count = database.select().fromType("T").where()//
        .property("n").eq().value(-1)//
        .or().property("a").neq().value("nonexistent").count();

    assertThat(count).isEqualTo(ROWS);
  }

  @Test
  void orWithLikeOnOneSideMustNotDropTheOtherBranch() {
    final long count = database.select().fromType("T").where()//
        .property("a").like().value("x")//
        .or().property("n").eq().value(-1).count();

    assertThat(count).isEqualTo(ROWS);
  }

  @Test
  void orWithIlikeOnOneSideMustNotDropTheOtherBranch() {
    final long count = database.select().fromType("T").where()//
        .property("a").ilike().value("X")//
        .or().property("n").eq().value(-1).count();

    assertThat(count).isEqualTo(ROWS);
  }

  @Test
  void orWithNeqOnBothSidesFallsBackToFullScanCorrectly() {
    // NEITHER SIDE IS CURSOR-BUILDABLE, SO THIS MUST FALL ALL THE WAY BACK TO A FULL SCAN - STILL CORRECT
    final long count = database.select().fromType("T").where()//
        .property("a").neq().value("nonexistent")//
        .or().property("n").neq().value(-999999).count();

    assertThat(count).isEqualTo(ROWS);
  }
}
