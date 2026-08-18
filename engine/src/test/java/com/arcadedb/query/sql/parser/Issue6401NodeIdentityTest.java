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
package com.arcadedb.query.sql.parser;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6401, items 3 and 4: the identity of a parsed node.
 * <p>
 * Two independent defects met in the same place. {@link SimpleNode#hashCode()} hashed the ARRAY that
 * {@code getIdentityElements()} returns rather than its contents, and that array is allocated fresh on every call - so
 * one node answered a different hash on every call, and two nodes {@code equals()} to each other answered different
 * hashes, which is the {@link Object#hashCode()} contract broken outright. And the identity of an expression left out
 * fields that carry its VALUE - {@code number} and {@code expression} on {@link BaseExpression}, {@code whereCondition}
 * and the inherited {@code value} slot on {@link Expression} - so the parsed forms of {@code SELECT 1} and
 * {@code SELECT 2} were {@code equals()} to each other, and so was every setting key of a {@code WITH} clause.
 * <p>
 * The two are each other's cover: {@code equals()} said yes where it should have said no, and the hash codes said no
 * to everything, so a {@code HashMap} keyed on a node behaved almost correctly by accident. Fixing either one alone
 * makes a map worse rather than better, which is why both are here.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6401NodeIdentityTest extends AbstractParserTest {

  private Expression projectionOf(final String query) {
    return ((SelectStatement) checkRightSyntax(query)).getProjection().getItems().getFirst().expression;
  }

  /**
   * The contract, on one object: a hash code is a property of the value, so asking twice has to answer twice the same.
   * It did not, because {@code Objects.hashCode(Object)} on an {@code Object[]} is the array's IDENTITY hash and the
   * array is a new one on every call.
   */
  @Test
  void oneNodeAnswersTheSameHashCodeOnEveryCall() {
    final Expression expression = projectionOf("SELECT 1 FROM V");
    assertThat(expression.hashCode()).isEqualTo(expression.hashCode());

    final SimpleNode statement = checkRightSyntax("SELECT a, b FROM V WHERE c > 3");
    assertThat(statement.hashCode()).isEqualTo(statement.hashCode());
  }

  /**
   * And the other half of the contract: two nodes that are equal answer the same hash code. This is the corpus the
   * issue #6401 javadoc above suggested widening to sweep for nodes whose identity does not work (issue #6409, item
   * 3). Widening it surfaced three: {@code foo[1..5]} exercises {@link ArrayRangeSelector}, confirmed by that issue
   * to fall back to reference identity and UNDER-match (two equal parses compared unequal); {@code ORDER BY} and
   * {@code INSERT ... SET} exercise {@link OrderByItem} and {@link InsertSetExpression}, which had the same gap; and
   * a bare {@code CaseExpression} had the opposite failure - no {@code getIdentityElements()} override of its own
   * meant it inherited {@link MathExpression}'s, which reads fields ({@code childExpressions}, {@code operators})
   * {@code CaseExpression} never populates, so it OVER-matched: two structurally different {@code CASE WHEN}
   * expressions compared equal to each other. The rest is a representative spread of statement and expression
   * shapes rather than an exhaustive walk of the ~67 {@code getIdentityElements()} overrides.
   */
  @Test
  void equalNodesAnswerEqualHashCodes() {
    for (final String query : new String[] { //
        "SELECT 1 FROM V", //
        "SELECT a FROM V", //
        "SELECT a.b[1] FROM V WHERE c = 'x'", //
        "SELECT (1 + 2) * 3 FROM V", //
        "SELECT max(a) FROM V GROUP BY b", //
        "SELECT FROM V WHERE a IN [1, 2, 3]", //
        "SELECT foo[1..5] FROM V", //
        "SELECT foo[1...5] FROM V", //
        "SELECT foo[?..?] FROM V", //
        "SELECT foo[:a..:b] FROM V", //
        "SELECT FROM V WHERE a = ?", //
        "SELECT FROM V WHERE a = :name", //
        "SELECT FROM V WHERE a BETWEEN 1 AND 10", //
        "SELECT FROM V WHERE a LIKE 'foo%'", //
        "SELECT FROM V ORDER BY a DESC LIMIT 10 SKIP 5", //
        "SELECT CASE WHEN a = 1 THEN 'x' ELSE 'y' END FROM V", //
        "SELECT CASE a WHEN 1 THEN 'x' ELSE 'y' END FROM V", //
        "UPDATE V SET a = 1 WHERE b = 2", //
        "DELETE FROM V WHERE a = 1", //
        "INSERT INTO V SET a = 1", //
        "REBUILD INDEX idx WITH batchSize = 100", //
        "REBUILD TYPE Foo WITH batchSize = 100, repartition = true", //
        "IMPORT DATABASE http://foo.bar WITH forceDatabaseCreate = true", //
        "EXPORT DATABASE file://foo.bar WITH format = 'graphml'", //
        "BACKUP DATABASE file://foo.bar WITH compressionLevel = 5" }) {
      final SimpleNode one = checkRightSyntax(query);
      final SimpleNode other = checkRightSyntax(query);

      assertThat(one).as(query).isEqualTo(other);
      assertThat(one.hashCode()).as(query).isEqualTo(other.hashCode());
    }
  }

  /**
   * The inverse of {@link #equalNodesAnswerEqualHashCodes()}: two DIFFERENT {@code CASE WHEN} expressions must
   * compare unequal. This is the one the corpus test above cannot express - it only ever compares a query against
   * itself - and it is the one that actually caught the {@link CaseExpression} defect (issue #6409, item 3): before
   * the fix, every {@code CASE} expression's identity was the two EMPTY lists it inherited from
   * {@link MathExpression}, so any two of them were equal regardless of their WHEN/THEN content.
   */
  @Test
  void differentCaseExpressionsAreNotTheSameExpression() {
    final Expression one = projectionOf("SELECT CASE WHEN a = 1 THEN 'x' ELSE 'y' END FROM V");
    final Expression two = projectionOf("SELECT CASE WHEN a = 2 THEN 'p' ELSE 'q' END FROM V");

    assertThat(one).as("different WHEN/THEN branches are different expressions").isNotEqualTo(two);
    assertThat(projectionOf("SELECT CASE WHEN a = 1 THEN 'x' ELSE 'y' END FROM V")).as("but the same one is the same").isEqualTo(one);
  }

  /** A bare numeric literal: five of {@code BaseExpression}'s fields are null, and the value lived in the sixth. */
  @Test
  void twoDifferentNumericLiteralsAreNotTheSameExpression() {
    final Expression one = projectionOf("SELECT 1 FROM V");
    final Expression two = projectionOf("SELECT 2 FROM V");

    assertThat(one).as("1 is not 2").isNotEqualTo(two);
    assertThat(projectionOf("SELECT 1 FROM V")).as("but 1 is 1").isEqualTo(one);
  }

  /** The same for the other omitted field: a parenthesised sub-expression parked in {@code expression}. */
  @Test
  void twoDifferentWrappedSubExpressionsAreNotTheSameExpression() {
    final Expression one = projectionOf("SELECT (1 + 2) FROM V");
    final Expression two = projectionOf("SELECT (3 + 4) FROM V");

    assertThat(one).as("(1 + 2) is not (3 + 4)").isNotEqualTo(two);
    assertThat(projectionOf("SELECT (1 + 2) FROM V")).as("but (1 + 2) is (1 + 2)").isEqualTo(one);
  }

  /** Map literals and collections are parked in the same field, so they get the same treatment. */
  @Test
  void twoDifferentCollectionLiteralsAreNotTheSameExpression() {
    assertThat(projectionOf("SELECT [1, 2] FROM V")).isNotEqualTo(projectionOf("SELECT [3, 4] FROM V"));
    assertThat(projectionOf("SELECT {'a': 1} FROM V")).isNotEqualTo(projectionOf("SELECT {'a': 2} FROM V"));
  }

  /**
   * What the contract buys, and the shape the four {@code Map<Expression, Expression> settings} maps in the DDL
   * statements are built out of: a key put in is a key found back, and putting an equal key twice overwrites rather
   * than growing a second entry that {@code get()} can never reach.
   */
  @Test
  void anExpressionUsedAsAHashKeyBehavesLikeAKey() {
    final Map<Expression, Expression> settings = new HashMap<>();
    settings.put(projectionOf("SELECT batchSize FROM V"), projectionOf("SELECT 1 FROM V"));
    settings.put(projectionOf("SELECT batchSize FROM V"), projectionOf("SELECT 2 FROM V"));

    assertThat(settings).as("the same setting named twice is ONE setting").hasSize(1);
    assertThat(settings.get(projectionOf("SELECT batchSize FROM V")))
        .as("and the last spelling of it wins").isEqualTo(projectionOf("SELECT 2 FROM V"));

    final Set<Expression> distinct = new HashSet<>();
    distinct.add(projectionOf("SELECT 1 FROM V"));
    distinct.add(projectionOf("SELECT 1 FROM V"));
    distinct.add(projectionOf("SELECT 2 FROM V"));
    assertThat(distinct).as("two different literals are two elements, and two equal ones are one").hasSize(2);
  }

  /**
   * {@code PNumber} is the node the literal's value lives in, and it had no identity of its own - so listing
   * {@code number} among {@code BaseExpression}'s identity elements would have compared two literals by object
   * identity, which says "not equal" for two parses of the very same statement.
   */
  @Test
  void aNumberNodeComparesByItsValue() {
    final PNumber one = new PNumber();
    one.value = 42;
    final PNumber same = new PNumber();
    same.value = 42;
    final PNumber other = new PNumber();
    other.value = 43;

    assertThat(one).isEqualTo(same);
    assertThat(one.hashCode()).isEqualTo(same.hashCode());
    assertThat(one).isNotEqualTo(other);
    assertThat(one.copy()).as("a copy keeps the identity of what it copied").isEqualTo(one);
  }

  /**
   * The shape that turned item 3 from a trap into a live defect once the hash codes started agreeing with
   * {@code equals()}: before issue #6409 item 1, {@code IMPORT}, {@code EXPORT} and {@code BACKUP DATABASE} parked a
   * setting NAME in the node's inherited {@code value} slot rather than building an identifier expression out of it,
   * and {@code value} was not among {@link Expression}'s identity elements - so every key of a {@code WITH} clause
   * was a node whose every other field is {@code null}/{@code false}, which is to say equal to every other key.
   * <p>
   * The key is now built the same way {@code REBUILD INDEX} and {@code REBUILD TYPE} already built it - {@code new
   * Expression(Identifier)} - so it is read back with {@code toString()}, not the raw {@code value} slot.
   */
  @Test
  void everySettingKeyOfAWithClauseIsItsOwnKey() {
    final ImportDatabaseStatement statement = (ImportDatabaseStatement) checkRightSyntax(
        "IMPORT DATABASE http://www.foo.bar WITH forceDatabaseCreate = true, commitEvery = 10000");

    assertThat(statement.settings).as("two settings are two entries").hasSize(2);
    assertThat(statement.settings.keySet().stream().map(Expression::toString))
        .containsExactlyInAnyOrder("forceDatabaseCreate", "commitEvery");
  }

  /**
   * {@code whereCondition} is the other field {@link Expression#execute} reads that identity used to ignore: a
   * parenthesised boolean condition lives there, and two different ones were the same node.
   */
  @Test
  void twoDifferentInlineConditionsAreNotTheSameExpression() {
    final Expression one = projectionOf("SELECT (a = 1 AND b = 2) FROM V");
    final Expression two = projectionOf("SELECT (a = 1 AND b = 3) FROM V");

    assertThat(one.whereCondition).as("the shape under test parks the condition in whereCondition").isNotNull();
    assertThat(one).isNotEqualTo(two);
    assertThat(projectionOf("SELECT (a = 1 AND b = 2) FROM V")).isEqualTo(one);
  }

  /** A node with no identity of its own keeps the default: itself, and stably so. */
  @Test
  void aNodeWithoutIdentityElementsStillHonoursTheContract() {
    final Timeout timeout = new Timeout();
    assertThat(timeout.hashCode()).isEqualTo(timeout.hashCode());
    assertThat(timeout).isEqualTo(timeout);
  }
}
