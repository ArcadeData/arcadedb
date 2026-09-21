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
import com.arcadedb.index.MultiIndexCursor;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * https://github.com/ArcadeData/arcadedb/issues/8059
 * <p>
 * Every other operator in {@code SelectOperator} passes its operands through
 * {@code SelectExecutor.evaluateValue()}, which is what turns a nested {@code SelectTreeNode} into a value.
 * {@code not} did not: its body was {@code left == Boolean.FALSE}, a reference comparison against the RAW,
 * unevaluated left operand. In a real tree that operand is a {@code SelectTreeNode}, never {@code Boolean.FALSE},
 * so the expression was false for every record and the whole query answered nothing.
 * <p>
 * {@code not} is UNARY in this engine - every in-tree use builds
 * {@code new SelectTreeNode(operand, SelectOperator.not, null)} - while a JSON condition is by construction the
 * binary triple {@code [left, operator, right]}. {@code Select.json()} resolved {@code not} by name through
 * {@code SelectOperator.byName()} and routed it through {@code setLogic()} exactly like {@code and}/{@code or}, so
 * the one path that could reach it built a shape with no defined meaning and handed the caller a silently empty
 * result set. It is now refused with a message that says why.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue8059SelectNotOperatorTest extends TestHelper {

  @Override
  protected void beginTest() {
    final DocumentType type = database.getSchema().createDocumentType("D");
    type.createProperty("a", Type.INTEGER);
    type.createProperty("b", Type.INTEGER);
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a");

    // FOUR RECORDS, ONE PER (a, b) COMBINATION OF 0/1
    database.transaction(() -> {
      for (int a = 0; a < 2; a++)
        for (int b = 0; b < 2; b++) {
          final MutableDocument doc = database.newDocument("D");
          doc.set("a", a);
          doc.set("b", b);
          doc.save();
        }
    });
  }

  @Test
  void notNegatesItsOperandInsteadOfAnsweringFalseForEveryRecord() {
    // NOT (a = 1): TWO OF THE FOUR RECORDS CARRY a = 0. THE OLD BODY ANSWERED false FOR ALL FOUR
    final Select select = database.select().fromType("D");
    final SelectTreeNode eqLeaf = new SelectTreeNode(new SelectPropertyValue("a"), SelectOperator.eq, 1);
    select.rootTreeElement = new SelectTreeNode(eqLeaf, SelectOperator.not, null);

    assertThat(new SelectExecutor(select).executeCount()).isEqualTo(2);
  }

  @Test
  void notOverAConjunctionNegatesTheWholeConjunction() {
    // NOT (a = 1 AND b = 1): THREE OF THE FOUR RECORDS FAIL THE CONJUNCTION
    final Select select = database.select().fromType("D");
    final SelectTreeNode conjunction = new SelectTreeNode(new SelectTreeNode(new SelectPropertyValue("a"), SelectOperator.eq, 1),
        SelectOperator.and, new SelectTreeNode(new SelectPropertyValue("b"), SelectOperator.eq, 1));
    select.rootTreeElement = new SelectTreeNode(conjunction, SelectOperator.not, null);

    assertThat(new SelectExecutor(select).executeCount()).isEqualTo(3);
  }

  @Test
  void anIndexedLeafBelowANotAncestorNeverBuildsACursor() {
    // #8048's PATH RULE, THE not HALF: THE LEAF'S PARENT HERE IS THE and, THE not IS ONE LEVEL FURTHER UP, AND THE
    // OLD DIRECT-PARENT CHECK MISSED IT. A POSITIVE CURSOR FOR a = 1 WOULD YIELD EXACTLY THE RECORDS THE NEGATION
    // REJECTS, SO THE ONLY SAFE PLAN IS A FULL SCAN
    final Select select = database.select().fromType("D");
    final SelectTreeNode conjunction = new SelectTreeNode(new SelectTreeNode(new SelectPropertyValue("a"), SelectOperator.eq, 1),
        SelectOperator.and, new SelectTreeNode(new SelectPropertyValue("b"), SelectOperator.eq, 1));
    select.rootTreeElement = new SelectTreeNode(conjunction, SelectOperator.not, null);

    final SelectExecutor executor = new SelectExecutor(select);
    final MultiIndexCursor cursor = executor.lookForIndexes();
    try {
      assertThat((Object) cursor).isNull();
      assertThat(executor.metrics().get("usedIndexes")).isEqualTo(0);
    } finally {
      if (cursor != null)
        cursor.close();
    }
  }

  @Test
  void aNotBranchIsNeverReportedAsIndexedToItsOrSibling() {
    // a = 0 OR NOT (a = 1): THE not BRANCH CONTRIBUTES NO CURSOR, SO IT MUST NOT BE REPORTED AS "INDEXED" TO THE
    // LEFT LEAF - OTHERWISE THE a = 0 CURSOR BECOMES THE WHOLE CANDIDATE SET AND THE NEGATED BRANCH IS DROPPED.
    // THE PREDICATE IS SATISFIED BY EVERY RECORD WHOSE a IS NOT 1, WHICH IS THE TWO a = 0 ONES
    final Select select = database.select().fromType("D");
    final SelectTreeNode notBranch = new SelectTreeNode(new SelectTreeNode(new SelectPropertyValue("a"), SelectOperator.eq, 1),
        SelectOperator.not, null);
    select.rootTreeElement = new SelectTreeNode(new SelectTreeNode(new SelectPropertyValue("a"), SelectOperator.eq, 0),
        SelectOperator.or, notBranch);

    final SelectExecutor executor = new SelectExecutor(select);
    final MultiIndexCursor cursor = executor.lookForIndexes();
    try {
      assertThat((Object) cursor).isNull();
    } finally {
      if (cursor != null)
        cursor.close();
    }

    assertThat(new SelectExecutor(select).executeCount()).isEqualTo(2);
  }

  @Test
  void jsonRefusesTheBinaryNotInsteadOfReturningNothing() {
    final JSONObject json = new JSONObject(
        "{\"fromType\":\"D\",\"where\":[[\":a\",\"=\",1],\"not\",[\":b\",\"=\",1]]}");

    assertThatThrownBy(() -> database.select().json(json)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'not' is unary");
  }

  @Test
  void jsonRefusesAnUnknownOperatorInsteadOfThrowingNullPointer() {
    final JSONObject json = new JSONObject("{\"fromType\":\"D\",\"where\":[[\":a\",\"=\",1],\"nand\",[\":b\",\"=\",1]]}");

    assertThatThrownBy(() -> database.select().json(json)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported operator 'nand'");
  }

  @Test
  void theOrControlFromTheIssueStillAnswersThreeRecords() {
    final JSONObject json = new JSONObject("{\"fromType\":\"D\",\"where\":[[\":a\",\"=\",1],\"or\",[\":b\",\"=\",1]]}");

    assertThat(database.select().json(json).count()).isEqualTo(3);
  }
}
