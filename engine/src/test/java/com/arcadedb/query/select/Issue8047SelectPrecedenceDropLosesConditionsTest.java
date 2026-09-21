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
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * https://github.com/ArcadeData/arcadedb/issues/8047
 * <p>
 * {@code Select.setLogic()}'s "the new operator does not bind more tightly" branch re-parented the current subtree
 * TWICE: the {@code SelectTreeNode} constructor already does it (and already moves the grandparent's child pointer
 * over), and the branch then called {@code newNode.setParent(currentParent.getParent())} - by which point
 * {@code currentParent.getParent()} IS {@code newNode}, so the call reduced to {@code newNode.setParent(newNode)}
 * and installed a self-parent cycle.
 * <p>
 * A self-parented node defeats the NEXT re-parenting: {@code setParent} rewires the grandparent by testing
 * {@code this.parent.left == this} / {@code this.parent.right == this}, and on such a node both tests compare it
 * against its own children and fail, so the real grandparent's {@code right} pointer never moved. The new node -
 * and everything appended to it afterwards - dangled off the tree {@code rootTreeElement} refers to, and the
 * executor never saw it. The shape that reaches the branch is a precedence DROP (an {@code or}) followed by three
 * or more {@code and}s, so from the fifth condition on every remaining condition was discarded in silence and the
 * query returned MORE rows than the predicate allows. A pure {@code and} chain and a pure {@code or} chain of any
 * length were unaffected, which is why the existing tests never caught it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue8047SelectPrecedenceDropLosesConditionsTest extends TestHelper {

  private static final String[] PROPERTIES = { "a", "b", "c", "d", "e", "f" };

  @Override
  protected void beginTest() {
    final DocumentType type = database.getSchema().createDocumentType("D");
    for (final String property : PROPERTIES)
      type.createProperty(property, Type.INTEGER);

    // EVERY COMBINATION OF THE SIX 0/1 FLAGS, SO EVERY PREDICATE BELOW HAS A NON-TRIVIAL, EXACTLY KNOWN ANSWER
    database.transaction(() -> {
      for (int mask = 0; mask < 1 << PROPERTIES.length; mask++) {
        final MutableDocument doc = database.newDocument("D");
        for (int bit = 0; bit < PROPERTIES.length; bit++)
          doc.set(PROPERTIES[bit], (mask >> bit) & 1);
        doc.save();
      }
    });
  }

  @Test
  void fifthConditionAfterAPrecedenceDropSurvivesIntoTheCompiledTree() {
    // THE TREE IS THE POINT: BEFORE THE FIX THE FIVE-CONDITION SELECT COMPILED TO EXACTLY THE SAME TREE AS THE
    // FOUR-CONDITION ONE, SO `and e = 1` WAS SIMPLY GONE. SelectCompiled.json() IS THE ENGINE'S OWN RENDERING OF
    // THE COMPILED TREE, SO THIS NEEDS NO INSTRUMENTATION TO SEE.
    final String fiveTerms = database.select().fromType("D").where()//
        .property("a").eq().value(1)//
        .or().property("b").eq().value(1)//
        .and().property("c").eq().value(1)//
        .and().property("d").eq().value(1)//
        .and().property("e").eq().value(1)//
        .compile().json().getJSONArray("where").toString();

    assertThat(fiveTerms).isEqualTo(
        "[[\":a\",\"=\",1],\"or\",[[[[\":b\",\"=\",1],\"and\",[\":c\",\"=\",1]],\"and\",[\":d\",\"=\",1]],\"and\",[\":e\",\"=\",1]]]");

    final String fourTerms = database.select().fromType("D").where()//
        .property("a").eq().value(1)//
        .or().property("b").eq().value(1)//
        .and().property("c").eq().value(1)//
        .and().property("d").eq().value(1)//
        .compile().json().getJSONArray("where").toString();

    assertThat(fourTerms).isNotEqualTo(fiveTerms);
  }

  @Test
  void orFollowedByFourAndsCountsTheSameAsSql() {
    final long native5 = database.select().fromType("D").where()//
        .property("a").eq().value(1)//
        .or().property("b").eq().value(1)//
        .and().property("c").eq().value(1)//
        .and().property("d").eq().value(1)//
        .and().property("e").eq().value(1).count();

    assertThat(native5).isEqualTo(sqlCount("a = 1 or b = 1 and c = 1 and d = 1 and e = 1"));
  }

  @Test
  void orFollowedByFiveAndsCountsTheSameAsSql() {
    // TWO CONDITIONS PAST THE BREAKING POINT: BEFORE THE FIX BOTH `e` AND `f` WERE DROPPED
    final long native6 = database.select().fromType("D").where()//
        .property("a").eq().value(1)//
        .or().property("b").eq().value(1)//
        .and().property("c").eq().value(1)//
        .and().property("d").eq().value(1)//
        .and().property("e").eq().value(1)//
        .and().property("f").eq().value(1).count();

    assertThat(native6).isEqualTo(sqlCount("a = 1 or b = 1 and c = 1 and d = 1 and e = 1 and f = 1"));
  }

  @Test
  void andThenOrThenThreeAndsCountsTheSameAsSql() {
    // THE OTHER SHAPE FROM THE ISSUE'S SWEEP: THE PRECEDENCE DROP IS NOT THE FIRST OPERATOR
    final long nativeCount = database.select().fromType("D").where()//
        .property("a").eq().value(1)//
        .and().property("b").eq().value(1)//
        .or().property("c").eq().value(1)//
        .and().property("d").eq().value(1)//
        .and().property("e").eq().value(1)//
        .and().property("f").eq().value(1).count();

    assertThat(nativeCount).isEqualTo(sqlCount("a = 1 and b = 1 or c = 1 and d = 1 and e = 1 and f = 1"));
  }

  @Test
  void pureAndAndPureOrChainsStayCorrect() {
    // THE CONTROL: THESE TWO NEVER REACHED THE BROKEN BRANCH AND MUST NOT REGRESS
    assertThat(database.select().fromType("D").where()//
        .property("a").eq().value(1)//
        .and().property("b").eq().value(1)//
        .and().property("c").eq().value(1)//
        .and().property("d").eq().value(1)//
        .and().property("e").eq().value(1).count()).isEqualTo(sqlCount("a = 1 and b = 1 and c = 1 and d = 1 and e = 1"));

    assertThat(database.select().fromType("D").where()//
        .property("a").eq().value(1)//
        .or().property("b").eq().value(1)//
        .or().property("c").eq().value(1)//
        .or().property("d").eq().value(1)//
        .or().property("e").eq().value(1).count()).isEqualTo(sqlCount("a = 1 or b = 1 or c = 1 or d = 1 or e = 1"));
  }

  @Test
  void aNodeCannotBecomeItsOwnParent() {
    // THE CYCLE IS WHAT TURNED A REDUNDANT CALL INTO A LOST CONDITION, AND IT ALSO MAKES EVERY getParent() WALK
    // NON-TERMINATING - SelectExecutor's OR/NOT ANCESTOR CHECK (#8048) DOES EXACTLY SUCH A WALK
    final SelectTreeNode node = new SelectTreeNode(new SelectPropertyValue("a"), SelectOperator.eq, 1);
    assertThatThrownBy(() -> node.setParent(node)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be its own parent");
  }

  private long sqlCount(final String where) {
    return ((Number) database.query("sql", "SELECT count(*) AS c FROM D WHERE " + where).nextIfAvailable().getProperty("c"))
        .longValue();
  }
}
