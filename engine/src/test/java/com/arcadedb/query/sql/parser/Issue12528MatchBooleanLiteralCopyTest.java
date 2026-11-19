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

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12528: a MATCH {@code where:} or {@code while:} filter that is a boolean literal was built as an anonymous
 * {@link BooleanExpression} with no {@code equals()}/{@code hashCode()}, so {@code copy()} was never equal to the statement it
 * copied and hashed differently, although both rendered the same. The statement round-trip contract of
 * {@link AbstractParserTest#checkRightSyntax} (rendering stable across a re-parse, a copy equal, same hash, same rendering)
 * must hold for these filters too.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12528MatchBooleanLiteralCopyTest extends AbstractParserTest {

  @ParameterizedTest
  @ValueSource(strings = {//
      "MATCH {as: a}.out('E'){while: (true), as: b} RETURN b",//
      "MATCH {as: a}.out('E'){while: (false), as: b} RETURN b",//
      "MATCH {as: a}.out('E'){while: true, as: b} RETURN b",//
      "MATCH {as: a}.out('E'){while: (true), where: (false), as: b} RETURN b",//
      "MATCH {as: a, where: (true)} RETURN a",//
      "MATCH {as: a, where: (false)} RETURN a",//
      "MATCH {as: a, where: true} RETURN a",//
      "MATCH {as: a, where: (((false)))} RETURN a",//
      "MATCH {type: V, as: a, where: (true)}.out('E'){as: b, where: (false)} RETURN a, b",//
      "MATCH {as: a}.out('E'){while: ($depth < 4), as: b} RETURN b",//
      "MATCH {as: a, where: (name = 'x')} RETURN a"})
  void copyOfAMatchStatementIsEqualToTheOriginal(final String sql) {
    checkRightSyntax(sql);
  }

  @Test
  void reportedStatementsCopyEqualsOriginal() {
    for (final String sql : new String[] { "MATCH {as: a}.out('E'){while: (true), as: b} RETURN a",
        "MATCH {as: a, where: (true)} RETURN a", "MATCH {as: a, where: (false)} RETURN a" }) {
      final Statement statement = new SQLAntlrParser(null).parse(sql);
      final Statement copy = statement.copy();
      assertThat(copy).as(sql).isEqualTo(statement);
      assertThat(copy.hashCode()).as(sql).isEqualTo(statement.hashCode());
    }
  }

  @Test
  void differentLiteralsAreNotEqual() {
    final Statement whenTrue = new SQLAntlrParser(null).parse("MATCH {as: a, where: (true)} RETURN a");
    final Statement whenFalse = new SQLAntlrParser(null).parse("MATCH {as: a, where: (false)} RETURN a");
    assertThat(whenTrue).isNotEqualTo(whenFalse);
  }

  @Test
  void theLiteralNodeKeepsItsValueThroughACopy() {
    final BooleanExpression literal = originFilterOf("MATCH {as: a, where: (false)} RETURN a").baseExpression;
    assertThat(literal.evaluate((Identifiable) null, null)).isFalse();
    assertThat(literal.copy().evaluate((Identifiable) null, null)).isFalse();
    assertThat(literal.copy()).isEqualTo(literal);
  }

  private static WhereClause originFilterOf(final String sql) {
    final MatchStatement statement = (MatchStatement) new SQLAntlrParser(null).parse(sql);
    return statement.matchExpressions.getFirst().origin.getFilter();
  }
}
