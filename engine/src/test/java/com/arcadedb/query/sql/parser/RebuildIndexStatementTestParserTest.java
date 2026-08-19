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

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RebuildIndexStatementTestParserTest extends AbstractParserTest {

  @Test
  void plain() {
    checkRightSyntax("REBUILD INDEX *");
    checkRightSyntax("REBUILD INDEX Foo");
    checkRightSyntax("rebuild index Foo");
    checkRightSyntax("REBUILD INDEX `Foo.bar`");
    checkRightSyntax("REBUILD INDEX `Foo.bar.baz`");
    checkRightSyntax("REBUILD INDEX * with batchSize = 1000");
    checkRightSyntax("REBUILD INDEX `Foo.bar.baz` with batchSize = 1000");

    checkWrongSyntax("REBUILD INDEX `Foo.bar` foo");
    checkRightSyntax("REBUILD INDEX `Foo.bar.baz` with unknown = 1000");
  }

  @Test
  void wildcardCapturesFirstSettingKey() throws Exception {
    // Regression for the firstSettingKeyIndex offset fix: the * (all) form has no index-name identifier, so the first WITH
    // setting (batchSize here) must still be parsed into the settings map - before the fix it was silently dropped. checkRightSyntax
    // only proves the grammar accepts it; this asserts the setting actually reaches the AST.
    final Statement stmt = new SQLAntlrParser(null).parse("REBUILD INDEX * WITH batchSize = 1000");
    assertThat(stmt).isInstanceOf(RebuildIndexStatement.class);
    final RebuildIndexStatement rebuild = (RebuildIndexStatement) stmt;
    assertThat(rebuild.all).isTrue();
    assertThat(rebuild.settings.keySet()).anyMatch(k -> "batchSize".equals(k.toString()));

    // Sanity: the named form still captures its setting too (identifier(0) is the index name, the key starts at identifier(1)).
    final RebuildIndexStatement named = (RebuildIndexStatement) new SQLAntlrParser(null).parse(
        "REBUILD INDEX `Foo.bar` WITH batchSize = 1000");
    assertThat(named.all).isFalse();
    assertThat(named.settings.keySet()).anyMatch(k -> "batchSize".equals(k.toString()));
  }

  /**
   * Regression test for issue #6428, item 1: {@code toString()} rendered the index name/{@code *} only, never
   * {@code settings}, so any code path that re-serializes the statement (statement caching keyed on text, query
   * logging, an audit trail) would silently lose the {@code WITH} clause - same defect as
   * {@code Export}/{@code BackupDatabaseStatement}, found in the same statement family while fixing those two.
   * <p>
   * Parsed with {@link #checkSyntax(String, boolean)} rather than {@link #checkRightSyntax(String)} on purpose:
   * this test asserts the round trip preserves {@code settings}, so it needs the pre-fix {@code toString()} output
   * to compare against, not just proof that whatever comes out still parses.
   */
  @Test
  void toStringRoundTripsTheWithSettings() {
    final SimpleNode parsed = checkSyntax("REBUILD INDEX `Foo.bar.baz` WITH batchSize = 1000, maxAttempts = 3", true);
    final RebuildIndexStatement original = (RebuildIndexStatement) parsed;

    final StringBuilder builder = new StringBuilder();
    original.toString(null, builder);
    assertThat(builder.toString()).contains("WITH");

    final RebuildIndexStatement roundTripped = (RebuildIndexStatement) checkSyntax(builder.toString(), true);
    assertThat(renderSettings(roundTripped)).isEqualTo(renderSettings(original));
    assertThat(roundTripped.name).isEqualTo(original.name);
  }

  /**
   * Regression test for the same statement family's {@code copy()}/{@code equals()} gap found alongside item 1:
   * {@code copy()} never carried {@code settings} over (silently dropping every {@code WITH} setting, the same
   * defect {@code Export}/{@code Backup}/{@code ImportDatabaseStatement} had, #6080/#6409) and {@code equals()}/
   * {@code hashCode()} never compared them (so two rebuilds with different settings compared equal, the same
   * over-match direction found for {@code ImportDatabaseStatement} in #6409's identity sweep).
   */
  @Test
  void copyAndEqualsKeepTheWithSettings() {
    final RebuildIndexStatement original = (RebuildIndexStatement) checkSyntax(
        "REBUILD INDEX `Foo.bar.baz` WITH batchSize = 1000, maxAttempts = 3", true);

    final RebuildIndexStatement copy = (RebuildIndexStatement) original.copy();
    assertThat(renderSettings(copy)).isEqualTo(renderSettings(original));
    assertThat(copy).isEqualTo(original);
    assertThat(copy.hashCode()).isEqualTo(original.hashCode());

    final RebuildIndexStatement differentSettings = (RebuildIndexStatement) checkSyntax(
        "REBUILD INDEX `Foo.bar.baz` WITH batchSize = 2000", true);
    assertThat(differentSettings).isNotEqualTo(original);
  }

  private static Map<String, String> renderSettings(final RebuildIndexStatement statement) {
    final Map<String, String> rendered = new HashMap<>();
    for (final Map.Entry<Expression, Expression> entry : statement.settings.entrySet())
      rendered.put(entry.getKey().toString(), entry.getValue().toString());
    return rendered;
  }
}
