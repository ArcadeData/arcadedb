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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * https://github.com/ArcadeData/arcadedb/issues/8167 and
 * https://github.com/ArcadeData/arcadedb/issues/8173
 * <p>
 * The JSON array is the ONLY way to hand the native Select API an explicitly grouped condition - the fluent builder
 * appends leaves left to right and has no grouping call - and it was the one input shape that silently lost the
 * grouping. {@code parseJsonCondition} recursed into a nested operand and replayed its leaves through
 * {@code setLogic}, the fluent builder's precedence machine, which re-applies operator precedence and throws the
 * JSON's own bracketing away: {@code a = 2 and (b = 1 or b = 3)} ran as {@code (a = 2 and b = 1) or b = 3}, with no
 * exception and no warning (#8167).
 * <p>
 * Nesting is now structural - a nested array is parsed into its own subtree and grafted in as one opaque operand,
 * which is exactly what the parentheses in the equivalent SQL do - so the native answer and the SQL answer agree,
 * and {@code SelectCompiled.json()} / {@code Select.json(JSONObject)} are inverse for a grouped tree as #6817
 * requires.
 * <p>
 * The same parse also refused what its own serialiser writes for a unary {@code not}: {@code toJSON()} emits a
 * TWO-element array, the arity gate demanded exactly three, and the specific message #8059 added for {@code not} sat
 * below the gate and was unreachable for that shape (#8173). The unary form is now read back, which closes the round
 * trip for every operator rather than all but one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue8167SelectJsonConditionTreeTest extends TestHelper {

  public Issue8167SelectJsonConditionTreeTest() {
    autoStartTx = false;
  }

  @Override
  protected void beginTest() {
    final DocumentType type = database.getSchema().createDocumentType("D");
    type.createProperty("a", Type.INTEGER);
    type.createProperty("b", Type.INTEGER);

    database.transaction(() -> {
      for (int a = 0; a < 3; a++)
        for (int b = 0; b < 4; b++) {
          final MutableDocument doc = database.newDocument("D");
          doc.set("a", a);
          doc.set("b", b);
          doc.save();
        }
    });
  }

  /**
   * The issue's own repro. 12 documents, {@code a} in 0..2 crossed with {@code b} in 0..3, so
   * {@code a = 2 and (b = 1 or b = 3)} matches 2 and the re-associated {@code (a = 2 and b = 1) or b = 3} matches 4.
   */
  @Test
  void aNestedOrOnTheRightOfAnAndKeepsItsGrouping() {
    assertNativeMatchesSQL("[[\":a\",\"=\",2],\"and\",[[\":b\",\"=\",1],\"or\",[\":b\",\"=\",3]]]",
        "a = 2 and (b = 1 or b = 3)");
  }

  @Test
  void aTwiceNestedTreeKeepsEveryLevelOfItsGrouping() {
    assertNativeMatchesSQL(
        "[[\":a\",\"=\",1],\"or\",[[\":a\",\"=\",2],\"and\",[[\":b\",\"=\",1],\"or\",[\":b\",\"=\",3]]]]",
        "a = 1 or (a = 2 and (b = 1 or b = 3))");
  }

  /**
   * The control: {@code or} is associative, so a nested {@code or} under an {@code or} is the one shape where the
   * re-association was harmless. It has to keep answering the same number.
   */
  @Test
  void aNestedOrUnderAnOrIsUnchanged() {
    assertNativeMatchesSQL("[[\":a\",\"=\",1],\"or\",[[\":b\",\"=\",1],\"or\",[\":a\",\"=\",2]]]",
        "a = 1 or (b = 1 or a = 2)");
  }

  @Test
  void aNestedAndOnTheLeftOfAnOrKeepsItsGrouping() {
    assertNativeMatchesSQL("[[[\":a\",\"=\",2],\"and\",[\":b\",\"=\",1]],\"or\",[\":b\",\"=\",3]]",
        "(a = 2 and b = 1) or b = 3");
  }

  /**
   * #8167/#6817: the grouped tree the caller wrote has to be RECOVERABLE. Before the fix the first round trip
   * already returned a different tree, so the second one was not the select the caller asked for either.
   */
  @Test
  void aGroupedTreeRoundTripsUnchanged() {
    for (final String where : new String[] {
        "[[\":a\",\"=\",2],\"and\",[[\":b\",\"=\",1],\"or\",[\":b\",\"=\",3]]]",
        "[[\":a\",\"=\",1],\"or\",[[\":a\",\"=\",2],\"and\",[[\":b\",\"=\",1],\"or\",[\":b\",\"=\",3]]]]",
        "[[[\":a\",\"=\",2],\"and\",[\":b\",\"=\",1]],\"or\",[\":b\",\"=\",3]]" }) {
      final JSONObject json = new JSONObject("{\"fromType\":\"D\",\"where\":" + where + "}");
      final JSONObject compiled = database.select().json(json).compile().json();
      assertThat(compiled.getJSONArray("where").toString()).as("where %s", where)
          .isEqualTo(json.getJSONArray("where").toString());
      // And it stays put on a second pass.
      assertThat(database.select().json(compiled).compile().json().toString()).isEqualTo(compiled.toString());
    }
  }

  /**
   * #8173: {@code SelectCompiled.json()} writes a {@code not} node as a two-element array, and its own reader
   * refused it with the generic "Invalid condition" message. The operator itself has been correct since #8059; only
   * the serialise/parse pair did not close.
   */
  @Test
  void aUnaryNotRoundTripsThroughJson() {
    final Select select = database.select().fromType("D");
    select.rootTreeElement = new SelectTreeNode(new SelectTreeNode(new SelectPropertyValue("a"), SelectOperator.eq, 1),
        SelectOperator.not, null);

    final JSONObject json = select.compile().json();
    assertThat(json.getJSONArray("where").toString()).isEqualTo("[[\":a\",\"=\",1],\"not\"]");

    // 12 documents, 4 of them with a = 1, so the negation answers 8.
    final long direct = database.select().fromType("D").json(new JSONObject().put("where", json.getJSONArray("where")))
        .count();
    assertThat(direct).isEqualTo(8);

    final JSONObject roundTripped = database.select().json(json).compile().json();
    assertThat(roundTripped.toString()).isEqualTo(json.toString());
  }

  @Test
  void aNotNestedInsideALogicTreeAlsoRoundTrips() {
    final JSONObject json = new JSONObject(
        "{\"fromType\":\"D\",\"where\":[[\":b\",\"=\",0],\"or\",[[\":a\",\"=\",1],\"not\"]]}");

    // b = 0 (3 documents) or not a = 1 (8 documents, 2 of which also have b = 0) -> 9.
    assertThat(database.select().json(json).count()).isEqualTo(9);
    assertThat(database.select().json(json).compile().json().getJSONArray("where").toString())
        .isEqualTo(json.getJSONArray("where").toString());
  }

  /**
   * #8173: the arity gate no longer swallows the cause. Each refusal has to name the operator and its arity, because
   * the generic message pointed away from the problem.
   */
  @Test
  void arityErrorsNameTheirOwnCause() {
    assertThatThrownBy(() -> database.select()
        .json(new JSONObject("{\"fromType\":\"D\",\"where\":[[\":a\",\"=\",1],\"not\",[\":b\",\"=\",1]]}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'not' is unary");

    assertThatThrownBy(() -> database.select()
        .json(new JSONObject("{\"fromType\":\"D\",\"where\":[\":a\",\"=\"]}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("binary and requires a right operand");

    assertThatThrownBy(() -> database.select()
        .json(new JSONObject("{\"fromType\":\"D\",\"where\":[\":a\",\"=\",1,2]}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid condition");

    // A logic operator joins conditions, so a bare property under one is refused HERE rather than evaluated as a
    // Boolean later, which handed the caller a ClassCastException with nothing to act on.
    assertThatThrownBy(() -> database.select()
        .json(new JSONObject("{\"fromType\":\"D\",\"where\":[\":a\",\"and\",\":b\"]}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be a condition");

    // A between range of the wrong arity is refused where the JSON is read, not per record deep in evaluation.
    assertThatThrownBy(() -> database.select()
        .json(new JSONObject("{\"fromType\":\"D\",\"where\":[\":b\",\"between\",[1,2,3]]}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("exactly two values");
    assertThatThrownBy(() -> database.select()
        .json(new JSONObject("{\"fromType\":\"D\",\"where\":[\":b\",\"between\",1]}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("exactly two values");
  }

  /**
   * A single bare leaf, the commonest select of all, keeps working in both the wrapped shape the compiler writes and
   * the bare triple a caller may hand-write.
   */
  @Test
  void aSingleLeafStillParsesInBothShapes() {
    assertThat(database.select().json(new JSONObject("{\"fromType\":\"D\",\"where\":[[\":a\",\"=\",1]]}")).count())
        .isEqualTo(4);
    assertThat(database.select().json(new JSONObject("{\"fromType\":\"D\",\"where\":[\":a\",\"=\",1]}")).count())
        .isEqualTo(4);
  }

  /**
   * A parameter written on the LEFT of a condition used to be routed through the fluent builder's
   * {@code parameter()}, which assigns the RIGHT-hand slot, so it landed on the wrong side of the operator.
   */
  @Test
  void aParameterOnTheLeftBindsToTheLeftOperand() {
    // '1 < b' over b in 0..3 with 3 documents per value: 6 rows. Routed to the wrong side it reads 'b < 1': 3 rows.
    final JSONObject json = new JSONObject("{\"fromType\":\"D\",\"where\":[\"#p\",\"<\",\":b\"]}");
    assertThat(database.select().json(json).compile().parameter("p", 1).count()).isEqualTo(6);
    // A bare triple is the non-canonical hand-written form: it canonicalizes to the run-wrapped shape the compiler
    // itself writes for a single leaf, which is then stable across further round trips.
    assertThat(database.select().json(json).compile().json().getJSONArray("where").toString())
        .isEqualTo("[[\"#p\",\"<\",\":b\"]]");
  }

  /**
   * Found alongside #8173: {@code between} and {@code in} write their right operand as a plain JSON array of
   * literals, which the reader took for a nested condition - so neither operator could be read back at all. The two
   * are now told apart by shape (a condition's middle element names an operator), and {@code between}'s bounds are
   * handed to the evaluator in the {@code Object[]} form it requires.
   */
  @Test
  void betweenAndInRoundTripAndStillRun() {
    final JSONObject between = database.select().fromType("D").where()//
        .property("b").between().values(1, 2).compile().json();
    assertThat(between.getJSONArray("where").toString()).isEqualTo("[[\":b\",\"between\",[1,2]]]");
    // b in {1,2}, 3 documents each.
    assertThat(database.select().json(between).count()).isEqualTo(6);
    assertThat(database.select().json(between).compile().json().toString()).isEqualTo(between.toString());

    final JSONObject in = database.select().fromType("D").where()//
        .property("b").in().value(List.of(0, 3)).compile().json();
    assertThat(database.select().json(in).count()).isEqualTo(6);
    assertThat(database.select().json(in).compile().json().toString()).isEqualTo(in.toString());
  }

  /**
   * The operand kind is decided by the OPERATOR, not by the operand's own shape, so a value list that happens to
   * carry an operator keyword in the position a condition's operator would occupy is still a value list. Told apart
   * by shape, {@code in ('red', 'in')} - a plausible tag list - read as a condition and was refused.
   */
  @Test
  void aValueListCarryingAnOperatorKeywordIsStillAValueList() {
    database.transaction(() -> {
      database.newDocument("D").set("a", 9, "b", 9, "tag", "red").save();
      database.newDocument("D").set("a", 9, "b", 9, "tag", "in").save();
      database.newDocument("D").set("a", 9, "b", 9, "tag", "green").save();
    });

    final JSONObject json = database.select().fromType("D").where()//
        .property("tag").in().value(List.of("red", "in")).compile().json();
    assertThat(json.getJSONArray("where").toString()).isEqualTo("[[\":tag\",\"in\",[\"red\",\"in\"]]]");
    assertThat(database.select().json(json).count()).isEqualTo(2);
    assertThat(database.select().json(json).compile().json().toString()).isEqualTo(json.toString());

    // Same shape for a two-element between range whose bounds are operator keywords.
    final JSONObject between = database.select().fromType("D").where()//
        .property("tag").between().values("and", "or").compile().json();
    assertThat(database.select().json(between).count())
        .isEqualTo(database.select().fromType("D").where().property("tag").between().values("and", "or").count());
    assertThat(database.select().json(between).compile().json().toString()).isEqualTo(between.toString());
  }

  /**
   * A literal on the LEFT of a comparison is refused on both sides of the change - a value array included, which
   * under a comparison is a list of values and never a nested condition.
   */
  @Test
  void aValueArrayIsNotAcceptedAsTheLeftOperandOfAComparison() {
    assertThatThrownBy(() -> database.select()
        .json(new JSONObject("{\"fromType\":\"D\",\"where\":[[1,2],\"=\",1]}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be a property or a parameter");
  }

  /**
   * A bare three-element condition at the top level is a ROOT LEAF, with no parent.
   * {@code SelectExecutor.filterWithIndexesFinalNode()} dereferences {@code node.getParent().operator} the moment a
   * leaf has a cursor-buildable index, and every tree the fluent builder produces has the synthetic {@code run} root
   * {@code compile()} adds - so the JSON reader has to produce that root too (found by CodeRabbit).
   */
  @Test
  void aBareRootConditionOnAnIndexedPropertyStillRuns() {
    database.getSchema().getType("D").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a");
    try {
      assertThat(database.select().json(new JSONObject("{\"fromType\":\"D\",\"where\":[\":a\",\"=\",1]}")).count())
          .isEqualTo(4);
      assertThat(database.select().json(new JSONObject("{\"fromType\":\"D\",\"where\":[[\":a\",\"=\",1]]}")).count())
          .isEqualTo(4);
    } finally {
      database.getSchema().getType("D").getIndexesByProperties("a").forEach(i -> database.getSchema().dropIndex(i.getName()));
    }
  }

  private void assertNativeMatchesSQL(final String where, final String sqlPredicate) {
    final JSONObject json = new JSONObject("{\"fromType\":\"D\",\"where\":" + where + "}");
    final long native_ = database.select().json(json).count();

    final long sql = database.query("sql", "SELECT count(*) AS c FROM D WHERE " + sqlPredicate).next()
        .<Number>getProperty("c").longValue();

    assertThat(native_).as("native select for %s", sqlPredicate).isEqualTo(sql);
  }
}
