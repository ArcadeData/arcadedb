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

import java.util.List;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SelectConditionSemanticsTest {

  /**
   * Regression for #8047: a precedence drop discarded the remaining conditions.
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
  @Nested
  class PrecedenceDropLosesConditions extends TestHelper {

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

  /**
   * Regression for #8049: lt/le matched records whose property is null.
   * https://github.com/ArcadeData/arcadedb/issues/8049
   * <p>
   * {@code SelectOperator.lt}/{@code le} compared through {@code BinaryComparator.compareTo()}, which is a SORT order
   * and answers {@code -1} for a null left operand. Read as a predicate that made a record WITHOUT the property - or
   * carrying it as an explicit null - satisfy every {@code <} and {@code <=} on it, while {@code gt}/{@code ge} were
   * unaffected by the same {@code -1}: the two directions disagreed.
   * <p>
   * Worse, the answer was not even stable. When an index exists on the property, {@code filterWithIndexesFinalNode()}
   * answers the leaf from a range scan, and an index holds no entry for a record that lacks the property - so the
   * null-bearing records were never offered to {@code evaluateWhere()} at all. The same select over the same data
   * returned 12 rows with no index and 5 with one, which is the one thing adding an index must never do.
   * <p>
   * Each predicate below is therefore asserted three ways: against SQL, without an index, and with one.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class NullOrderingComparison extends TestHelper {

    private static final int WITH_VALUE  = 5;
    private static final int WITHOUT_A   = 5;
    private static final int EXPLICIT_NULL = 2;

    @Override
    protected void beginTest() {
      final DocumentType type = database.getSchema().createDocumentType("D");
      type.createProperty("a", Type.INTEGER);
      type.createProperty("tag", Type.STRING);

      database.transaction(() -> {
        for (int i = 0; i < WITH_VALUE; i++) {
          final MutableDocument doc = database.newDocument("D");
          doc.set("a", i);
          doc.set("tag", "valued");
          doc.save();
        }
        for (int i = 0; i < WITHOUT_A; i++) {
          final MutableDocument doc = database.newDocument("D");
          doc.set("tag", "absent");
          doc.save();
        }
        for (int i = 0; i < EXPLICIT_NULL; i++) {
          final MutableDocument doc = database.newDocument("D");
          doc.set("a", null);
          doc.set("tag", "null");
          doc.save();
        }
      });
    }

    @Test
    void lessThanDoesNotMatchRecordsWithoutTheProperty() {
      assertSameWithAndWithoutIndex(() -> database.select().fromType("D").where().property("a").lt().value(5).count(),
          "a < 5", WITH_VALUE);
    }

    @Test
    void lessThanOrEqualDoesNotMatchRecordsWithoutTheProperty() {
      assertSameWithAndWithoutIndex(() -> database.select().fromType("D").where().property("a").le().value(4).count(),
          "a <= 4", WITH_VALUE);
    }

    @Test
    void greaterThanStaysConsistent() {
      assertSameWithAndWithoutIndex(() -> database.select().fromType("D").where().property("a").gt().value(-1).count(),
          "a > -1", WITH_VALUE);
    }

    @Test
    void greaterThanOrEqualStaysConsistent() {
      assertSameWithAndWithoutIndex(() -> database.select().fromType("D").where().property("a").ge().value(0).count(),
          "a >= 0", WITH_VALUE);
    }

    @Test
    void betweenStaysConsistent() {
      assertSameWithAndWithoutIndex(
          () -> database.select().fromType("D").where().property("a").between().values(0, 4).count(), "a between 0 and 4",
          WITH_VALUE);
    }

    @Test
    void isNullStillSeesEveryRecordWithoutAValue() {
      // THE COMPLEMENTARY QUESTION IS THE ONE THAT ANSWERS "WHICH RECORDS HAVE NO VALUE" - IT MUST KEEP ANSWERING IT
      assertThat(database.select().fromType("D").where().property("a").isNull().count()).isEqualTo(
          WITHOUT_A + EXPLICIT_NULL);
      assertThat(database.select().fromType("D").where().property("a").isNotNull().count()).isEqualTo(WITH_VALUE);
    }

    @Test
    void lessThanAndGreaterThanOrEqualAreNoLongerJointlyExhaustive() {
      // THE OLD BEHAVIOUR MADE `a < 5` AND `a >= 5` COVER EVERY RECORD WHILE `a is null` WAS ALSO TRUE FOR SOME OF
      // THEM. THE TWO HALVES NOW SUM TO THE RECORDS THAT ACTUALLY CARRY A VALUE, AND NO MORE
      final long below = database.select().fromType("D").where().property("a").lt().value(5).count();
      final long atOrAbove = database.select().fromType("D").where().property("a").ge().value(5).count();
      assertThat(below + atOrAbove).isEqualTo(WITH_VALUE);
    }

    @Test
    void aNullRightOperandNeverMatches() {
      // SQL'S OWN RULE: A COMPARISON WITH NULL IS UNKNOWN, AND UNKNOWN DOES NOT MATCH. compareTo(x, null) ANSWERS 1,
      // SO `a > null` USED TO BE TRUE FOR EVERY RECORD CARRYING AN 'a'
      assertThat(database.select().fromType("D").where().property("a").gt().value(null).count()).isZero();
      assertThat(database.select().fromType("D").where().property("a").ge().value(null).count()).isZero();
      assertThat(database.select().fromType("D").where().property("a").lt().value(null).count()).isZero();
      assertThat(database.select().fromType("D").where().property("a").le().value(null).count()).isZero();
    }

    private void assertSameWithAndWithoutIndex(final java.util.function.LongSupplier nativeCount, final String sqlWhere,
        final long expected) {
      assertThat(sqlCount(sqlWhere)).as("SQL baseline for " + sqlWhere).isEqualTo(expected);
      assertThat(nativeCount.getAsLong()).as("native without an index: " + sqlWhere).isEqualTo(expected);

      database.getSchema().getType("D").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a");
      try {
        assertThat(nativeCount.getAsLong()).as("native with an index: " + sqlWhere).isEqualTo(expected);
      } finally {
        for (final var index : database.getSchema().getType("D").getIndexesByProperties("a"))
          database.getSchema().dropIndex(index.getName());
      }
    }

    private long sqlCount(final String where) {
      return ((Number) database.query("sql", "SELECT count(*) AS c FROM D WHERE " + where).nextIfAvailable().getProperty("c"))
          .longValue();
    }
  }

  /**
   * Regression for #8059: the not operator was false for every record.
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
  @Nested
  class NotOperator extends TestHelper {

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

  /**
   * Regression for #8170: ilike threw NullPointerException on a record without the property.
   * https://github.com/ArcadeData/arcadedb/issues/8170
   * <p>
   * The native {@code ilike} folded the case of both operands without a null check, so a single record that does not
   * carry the property aborted the whole query with a {@link NullPointerException}, while {@code like} and SQL
   * {@code ILIKE} simply did not match it. Asserted three ways: against SQL, without an index and with one.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class IlikeMissingProperty extends TestHelper {

    @Override
    protected void beginTest() {
      final DocumentType type = database.getSchema().createDocumentType("D");
      type.createProperty("s", Type.STRING);

      database.transaction(() -> {
        final MutableDocument valued = database.newDocument("D");
        valued.set("s", "abc");
        valued.save();

        final MutableDocument absent = database.newDocument("D");
        absent.set("other", 1);
        absent.save();

        final MutableDocument explicitNull = database.newDocument("D");
        explicitNull.set("s", null);
        explicitNull.save();
      });
    }

    @Test
    void ilikeSkipsRecordsWithoutTheProperty() {
      assertSameWithAndWithoutIndex(() -> database.select().fromType("D").where().property("s").ilike().value("A%").count(),
          "s ILIKE 'A%'", 1);
    }

    @Test
    void likeStillSkipsRecordsWithoutTheProperty() {
      assertSameWithAndWithoutIndex(() -> database.select().fromType("D").where().property("s").like().value("a%").count(),
          "s LIKE 'a%'", 1);
    }

    @Test
    void ilikeWithANullPatternNeverMatches() {
      assertThat(database.select().fromType("D").where().property("s").ilike().value(null).count()).isZero();
    }

    @Test
    void nonStringAndMultiValueOperandsMatchLikeSql() {
      // THE NATIVE OPERATORS NOW EVALUATE THROUGH THE SQL ONES: A NON-STRING VALUE IS MATCHED BY ITS STRING FORM INSTEAD
      // OF FAILING A CAST, AND A LIST MATCHES WHEN ANY OF ITS ITEMS DOES
      database.getSchema().createDocumentType("M");
      database.transaction(() -> {
        database.newDocument("M").set("n", 123).set("tags", List.of("Alpha", "beta")).save();
        database.newDocument("M").set("n", 456).set("tags", List.of("gamma")).save();
      });

      assertThat(database.select().fromType("M").where().property("n").like().value("12%").count()).isEqualTo(
          sqlCount("M", "n LIKE '12%'")).isEqualTo(1);
      assertThat(database.select().fromType("M").where().property("n").ilike().value("45%").count()).isEqualTo(
          sqlCount("M", "n ILIKE '45%'")).isEqualTo(1);
      assertThat(database.select().fromType("M").where().property("tags").ilike().value("ALPHA").count()).isEqualTo(
          sqlCount("M", "tags ILIKE 'ALPHA'")).isEqualTo(1);
      assertThat(database.select().fromType("M").where().property("tags").like().value("gam%").count()).isEqualTo(
          sqlCount("M", "tags LIKE 'gam%'")).isEqualTo(1);
    }

    private void assertSameWithAndWithoutIndex(final LongSupplier nativeCount, final String sqlWhere, final long expected) {
      assertThat(sqlCount(sqlWhere)).as("SQL baseline for " + sqlWhere).isEqualTo(expected);
      assertThat(nativeCount.getAsLong()).as("native without an index: " + sqlWhere).isEqualTo(expected);

      database.getSchema().getType("D").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "s");
      try {
        assertThat(nativeCount.getAsLong()).as("native with an index: " + sqlWhere).isEqualTo(expected);
      } finally {
        for (final var index : database.getSchema().getType("D").getIndexesByProperties("s"))
          database.getSchema().dropIndex(index.getName());
      }
    }

    private long sqlCount(final String where) {
      return sqlCount("D", where);
    }

    private long sqlCount(final String type, final String where) {
      return ((Number) database.query("sql", "SELECT count(*) AS c FROM " + type + " WHERE " + where).nextIfAvailable()
          .getProperty("c")).longValue();
    }
  }
}
