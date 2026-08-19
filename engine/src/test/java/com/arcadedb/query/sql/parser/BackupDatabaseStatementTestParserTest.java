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

class BackupDatabaseStatementTestParserTest extends AbstractParserTest {

  @Test
  void plain() {
    checkRightSyntax("BACKUP DATABASE http://www.foo.bar");
    checkRightSyntax("backup database http://www.foo.bar");
    checkRightSyntax("BACKUP DATABASE https://www.foo.bar");
    checkRightSyntax("BACKUP DATABASE file:///foo/bar/");
    checkRightSyntax("backup database "); // USE THE DEFAULT FILE NAME

    // WITH clause settings
    checkRightSyntax("BACKUP DATABASE WITH compressionLevel = 5");
    checkRightSyntax("BACKUP DATABASE file:///foo/bar/ WITH compressionLevel = 5, maxMBPerSecond = 10");

    checkWrongSyntax("backup database file:///foo/bar/ foo bar");
    checkWrongSyntax("backup database http://www.foo.bar asdf ");
    checkWrongSyntax("BACKUP DATABASE https://www.foo.bar asd ");
  }

  @Test
  void isIdempotent() {
    final SimpleNode parsed = checkRightSyntax("backup database ");
    assertThat(parsed).isInstanceOf(BackupDatabaseStatement.class);
    assertThat(((BackupDatabaseStatement) parsed).isIdempotent()).isTrue();
  }

  /**
   * Regression test for issue #6428, item 1: {@code toString()} only rendered {@code url}, never {@code settings},
   * so any code path that re-serializes the statement (statement caching keyed on text, query logging, an audit
   * trail) would silently lose the {@code WITH} clause - the same shape as the historical cleartext-encryption bug
   * (#6080), but on the print path instead of the read path.
   * <p>
   * Parsed with {@link #checkSyntax(String, boolean)} rather than {@link #checkRightSyntax(String)} on purpose:
   * this test asserts the round trip preserves {@code settings}, so it needs the pre-fix {@code toString()} output
   * to compare against, not just proof that whatever comes out still parses.
   */
  @Test
  void toStringRoundTripsTheWithSettings() {
    final SimpleNode parsed = checkSyntax("BACKUP DATABASE file:///foo/bar/ WITH compressionLevel = 5, maxMBPerSecond = 10", true);
    final BackupDatabaseStatement original = (BackupDatabaseStatement) parsed;

    final StringBuilder builder = new StringBuilder();
    original.toString(null, builder);
    assertThat(builder.toString()).contains("WITH");

    final BackupDatabaseStatement roundTripped = (BackupDatabaseStatement) checkSyntax(builder.toString(), true);
    assertThat(renderSettings(roundTripped)).isEqualTo(renderSettings(original));
    assertThat(roundTripped.url).isEqualTo(original.url);
  }

  private static Map<String, String> renderSettings(final BackupDatabaseStatement statement) {
    final Map<String, String> rendered = new HashMap<>();
    for (final Map.Entry<Expression, Expression> entry : statement.settings.entrySet())
      rendered.put(entry.getKey().toString(), entry.getValue().toString());
    return rendered;
  }
}
