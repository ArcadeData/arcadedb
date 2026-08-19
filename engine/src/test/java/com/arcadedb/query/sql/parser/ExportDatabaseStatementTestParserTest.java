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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

class ExportDatabaseStatementTestParserTest extends AbstractParserTest {

  @Test
  void plain() {
    checkRightSyntax("EXPORT DATABASE http://www.foo.bar");
    checkRightSyntax("export database http://www.foo.bar");
    checkRightSyntax("EXPORT DATABASE https://www.foo.bar");
    checkRightSyntax("EXPORT DATABASE file:///foo/bar/");
    checkRightSyntax("export database ");

    // WITH clause settings
    checkRightSyntax("EXPORT DATABASE WITH overwrite = true");
    checkRightSyntax("EXPORT DATABASE WITH format = 'graphson'");
    checkRightSyntax("EXPORT DATABASE file://Movies.graphson.tgz WITH format = 'graphson', overwrite = true");

    checkWrongSyntax("export database file:///foo/bar/ foo bar");
    checkWrongSyntax("export database http://www.foo.bar asdf ");
    checkWrongSyntax("EXPORT DATABASE https://www.foo.bar asd ");
  }

  /**
   * Regression test for issue #6087: {@code copy()} only carried the URL over, so every {@code WITH ...} setting was
   * silently dropped by the copy. Same defect {@code BackupDatabaseStatement} had (#6080).
   * <p>
   * Parsed with {@link #checkSyntax(String, boolean)} rather than {@link #checkRightSyntax(String)} on purpose:
   * the latter re-parses the node's own {@code toString()}, and {@code ExportDatabaseStatement.toString()} does not
   * render the {@code WITH} clause, so the round-trip would hand back a statement with an empty settings map and the
   * test would assert nothing.
   */
  @Test
  void copyKeepsTheWithSettings() {
    final SimpleNode parsed = checkSyntax("EXPORT DATABASE file://Movies.graphson.tgz WITH format = 'graphson', overwrite = true",
        true);
    assertThat(parsed).isInstanceOf(ExportDatabaseStatement.class);

    final ExportDatabaseStatement original = (ExportDatabaseStatement) parsed;
    assertThat(renderSettings(original)).containsOnly(entry("format", "'graphson'"), entry("overwrite", "true"));

    final ExportDatabaseStatement copy = (ExportDatabaseStatement) original.copy();

    assertThat(renderSettings(copy)).isEqualTo(renderSettings(original));
    assertThat(copy.url).isEqualTo(original.url);
  }

  /**
   * Regression test for issue #6428, item 1: {@code toString()} only rendered {@code url}, never {@code settings},
   * so any code path that re-serializes the statement (statement caching keyed on text, query logging, an audit
   * trail) would silently lose the {@code WITH} clause.
   */
  @Test
  void toStringRoundTripsTheWithSettings() {
    final SimpleNode parsed = checkSyntax("EXPORT DATABASE file://Movies.graphson.tgz WITH format = 'graphson', overwrite = true",
        true);
    final ExportDatabaseStatement original = (ExportDatabaseStatement) parsed;

    final StringBuilder builder = new StringBuilder();
    original.toString(null, builder);
    assertThat(builder.toString()).contains("WITH");

    final ExportDatabaseStatement roundTripped = (ExportDatabaseStatement) checkSyntax(builder.toString(), true);
    assertThat(renderSettings(roundTripped)).isEqualTo(renderSettings(original));
    assertThat(roundTripped.url).isEqualTo(original.url);
  }

  @Test
  void copyOfAStatementWithoutSettingsKeepsAnEmptyMap() {
    final SimpleNode parsed = checkSyntax("EXPORT DATABASE file://Movies.jsonl.tgz", true);
    final ExportDatabaseStatement original = (ExportDatabaseStatement) parsed;
    assertThat(original.settings).isEmpty();

    final ExportDatabaseStatement copy = (ExportDatabaseStatement) original.copy();

    assertThat(copy.settings).isEmpty();
    assertThat(copy.url).isEqualTo(original.url);
  }

  /**
   * Renders the settings the way {@code executeSimple} consumes them: the setting name read back with
   * {@code toString()} against the rendered value. The key is built as {@code new Expression(Identifier)} - never
   * the raw-{@code value} shape this used to read (issue #6409, item 1) - so {@code toString()} is how every one of
   * the five {@code WITH}-settings statements recovers it now.
   */
  private static Map<String, String> renderSettings(final ExportDatabaseStatement statement) {
    final Map<String, String> rendered = new HashMap<>();
    for (final Map.Entry<Expression, Expression> entry : statement.settings.entrySet())
      rendered.put(entry.getKey().toString(), entry.getValue().toString());
    return rendered;
  }
}
