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

import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #9650: the four {@code Statement} subclasses whose {@code toString()} or {@code copy()} was still
 * lossy, unparseable or throwing after the twelve issues filed on that shape one at a time (#7789 to #7801).
 * <p>
 * The wider guard asked for by the issue lives in {@link AbstractParserTest#checkRightSyntax}: every statement any parser test
 * round-trips through it is now also checked for a stable re-render and for a {@code copy()} that is equal to, and renders the
 * same as, its original.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9650StatementRenderAndCopyTest extends AbstractParserTest {

  /** Row 1: the index type is optional in the grammar (validate() refuses its absence later), so the render must not assume it. */
  @Test
  void createIndexWithoutATypeRendersInsteadOfThrowing() {
    final Statement stmt = parse("CREATE INDEX ON Foo (a)");
    assertThat(stmt.toString()).isEqualTo("CREATE INDEX ON Foo (a) NULL_STRATEGY SKIP");
    assertRoundTrips(stmt);
  }

  @Test
  void explainOfACreateIndexWithoutATypeRenders() {
    final Statement stmt = parse("EXPLAIN CREATE INDEX ON Foo (a)");
    assertThat(stmt.toString()).isEqualTo("EXPLAIN CREATE INDEX ON Foo (a) NULL_STRATEGY SKIP");
  }

  /** Row 2: BreakStatement had no copy() and inherited the throwing base. */
  @Test
  void breakStatementCopies() {
    final Statement stmt = parse("IF (1 = 1) { BREAK; }");
    assertRoundTrips(stmt);

    final BreakStatement breakStatement = new BreakStatement();
    assertThat(breakStatement.copy()).isInstanceOf(BreakStatement.class);
    assertThat(breakStatement.copy().toString()).isEqualTo("BREAK");
  }

  /**
   * Row 3: MatchStatement.copy() ran buildPatterns() on the copy, which aliased every unaliased node and appended an empty filter
   * item that rendered as a trailing ", " - unparseable, and not equal to the original.
   */
  @Test
  void matchStatementCopyIsEqualAndParseable() {
    final MatchStatement stmt = (MatchStatement) parse("MATCH {type: 'V', as: foo} RETURN foo");
    final MatchStatement copy = stmt.copy();

    assertThat(copy.toString()).isEqualTo("MATCH {type: 'V', as: foo} RETURN foo");
    assertThat(copy).isEqualTo(stmt);
    assertThat(copy.hashCode()).isEqualTo(stmt.hashCode());
    assertThat(parse(copy.toString())).isEqualTo(stmt);
  }

  /** An unaliased node must not come back from copy() carrying a generated $ARCADEDB_DEFAULT_ALIAS_n alias. */
  @Test
  void matchStatementCopyDoesNotInventAliases() {
    final Statement stmt = parse("MATCH {type: 'V'}.out('E'){where: (name = 'x')} RETURN $elements");
    final Statement copy = stmt.copy();

    assertThat(copy.toString()).doesNotContain(MatchStatement.DEFAULT_ALIAS_PREFIX);
    assertThat(copy.toString()).isEqualTo(stmt.toString());
    assertThat(copy).isEqualTo(stmt);
  }

  @Test
  void matchSubqueryInsideLetCopies() {
    assertRoundTrips(parse("SELECT FROM V LET $x = (MATCH {type: 'V', as: foo} RETURN foo)"));
  }

  /** Clearing a filter that was never there is a no-op: it used to append an empty item that rendered as a dangling comma. */
  @Test
  void clearingAnAbsentMatchFilterAddsNothing() {
    final MatchStatement stmt = (MatchStatement) parse("MATCH {type: 'V', as: foo} RETURN foo");
    final MatchFilter filter = stmt.getMatchExpressions().getFirst().getOrigin();
    final String before = filter.toString();

    filter.setFilter(null);

    assertThat(filter.toString()).isEqualTo(before);
  }

  /** Row 4: the WITH settings map was a HashMap, so the clause re-rendered in arbitrary order, and differently from its own copy. */
  @Test
  void withSettingsRenderInDeclaredOrder() {
    assertRendersVerbatim("EXPORT DATABASE file://e.jsonl.tgz WITH format = 'jsonl', overwrite = true, includeTypes = Person");
    assertRendersVerbatim("IMPORT DATABASE file://e.jsonl.tgz WITH zeta = 1, commitEvery = 10, alpha = 'x', mid = true");
    assertRendersVerbatim("BACKUP DATABASE file://b.zip WITH maxMBPerSecond = 10, compressionLevel = 5, encryptionAlgorithm = 'AES'");
    assertRendersVerbatim("REBUILD INDEX Foo WITH zeta = 1, batchSize = 1000, alpha = 2");
    assertRendersVerbatim("REBUILD TYPE Foo POLYMORPHIC WITH repartition = true, batchSize = 1000, alpha = 2");
  }

  private void assertRendersVerbatim(final String sql) {
    final Statement stmt = parse(sql);
    assertThat(stmt.toString()).isEqualTo(sql);
    assertRoundTrips(stmt);
  }

  private static void assertRoundTrips(final Statement stmt) {
    final String rendered = stmt.toString();
    final Statement copy = stmt.copy();
    assertThat(copy.toString()).isEqualTo(rendered);
    assertThat(copy).isEqualTo(stmt);
    assertThat(copy.hashCode()).isEqualTo(stmt.hashCode());
    assertThat(parse(rendered).toString()).isEqualTo(rendered);
  }

  private static Statement parse(final String sql) {
    return new SQLAntlrParser(null).parse(sql);
  }
}
