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

class ImportDatabaseStatementTestParserTest extends AbstractParserTest {

  @Test
  void plain() {
    checkRightSyntax("IMPORT DATABASE http://www.foo.bar");
    checkRightSyntax("import database http://www.foo.bar");
    checkRightSyntax("IMPORT DATABASE https://www.foo.bar");
    checkRightSyntax("IMPORT DATABASE file:///foo/bar/");
    checkRightSyntax("IMPORT DATABASE http://www.foo.bar WITH forceDatabaseCreate = true");
    checkRightSyntax("IMPORT DATABASE http://www.foo.bar WITH forceDatabaseCreate = true, commitEvery = 10000");

    checkWrongSyntax("import database file:///foo/bar/ foo bar");
    checkWrongSyntax("import database http://www.foo.bar asdf ");
    checkWrongSyntax("IMPORT DATABASE https://www.foo.bar asd ");
  }

  /**
   * Regression test for GitHub issue #1552.
   * IMPORT DATABASE should allow optional URL when vertices/edges files are specified.
   */
  @Test
  void regressionIssue1552OptionalUrl() {
    // URL should be optional when using vertices/edges settings
    checkRightSyntax("IMPORT DATABASE WITH vertices=\"file://vertices.csv\"");
    checkRightSyntax("IMPORT DATABASE WITH vertices=\"file://vertices.csv\", verticesFileType=csv, typeIdProperty=Id");
    checkRightSyntax(
        """
        IMPORT DATABASE WITH vertices="file://vertices.csv", verticesFileType=csv, typeIdProperty=Id, \
        edges="file://edges.csv", edgesFileType=csv, edgeFromField="From", edgeToField="To"\
        """);
  }

  /**
   * Regression test for issue #6087: {@code copy()} only carried the URL over, so every {@code WITH ...} setting was
   * silently dropped by the copy. Same defect {@code BackupDatabaseStatement} had (#6080).
   */
  @Test
  void copyKeepsTheWithSettings() {
    final SimpleNode parsed = checkRightSyntax("IMPORT DATABASE http://www.foo.bar WITH forceDatabaseCreate = true, commitEvery = 10000");
    assertThat(parsed).isInstanceOf(ImportDatabaseStatement.class);

    final ImportDatabaseStatement original = (ImportDatabaseStatement) parsed;
    assertThat(renderSettings(original)).containsOnly(entry("forceDatabaseCreate", "true"), entry("commitEvery", "10000"));

    final ImportDatabaseStatement copy = (ImportDatabaseStatement) original.copy();

    assertThat(renderSettings(copy)).isEqualTo(renderSettings(original));
    assertThat(copy.url).isEqualTo(original.url);
  }

  @Test
  void copyOfAStatementWithoutSettingsKeepsAnEmptyMap() {
    final SimpleNode parsed = checkRightSyntax("IMPORT DATABASE http://www.foo.bar");
    final ImportDatabaseStatement original = (ImportDatabaseStatement) parsed;
    assertThat(original.settings).isEmpty();

    final ImportDatabaseStatement copy = (ImportDatabaseStatement) original.copy();

    assertThat(copy.settings).isEmpty();
    assertThat(copy.url).isEqualTo(original.url);
  }

  /**
   * Renders the settings the way {@code executeSimple} consumes them: the raw setting name held in
   * {@code Expression.value} against the rendered value.
   * <p>
   * Comparing the two {@code Map<Expression, Expression>} instances directly would not work: {@code SimpleNode}
   * derives {@code hashCode()} from a freshly allocated {@code Object[]}, so an {@code Expression} hashes to a
   * different bucket on every call and {@code HashMap.equals} can never find a key. That is a separate, pre-existing
   * defect - the settings map is only ever iterated in production, never looked up - and this test deliberately does
   * not depend on it either way.
   */
  private static Map<String, String> renderSettings(final ImportDatabaseStatement statement) {
    final Map<String, String> rendered = new HashMap<>();
    for (final Map.Entry<Expression, Expression> entry : statement.settings.entrySet())
      rendered.put(entry.getKey().value.toString(), entry.getValue().toString());
    return rendered;
  }
}
