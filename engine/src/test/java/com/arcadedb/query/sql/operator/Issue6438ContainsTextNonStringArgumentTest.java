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
package com.arcadedb.query.sql.operator;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6438: {@code CONTAINSTEXT} with a non-String right-hand argument meant one thing when a
 * full-text index covered the property (the index stringifies the key and searches normally) and another when it did
 * not ({@link com.arcadedb.query.sql.parser.ContainsTextCondition#evaluate} refused anything that was not already a
 * {@code String} and answered {@code false}). Both paths must now agree: a numeric (or otherwise non-String) literal
 * is coerced to its string form and searched like any other text.
 */
class Issue6438ContainsTextNonStringArgumentTest extends TestHelper {

  @Test
  void numericLiteralMatchesTheSameWayIndexedAndUnindexed() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Indexed6438");
      database.command("sql", "CREATE PROPERTY Indexed6438.content STRING");
      database.command("sql", "CREATE INDEX ON Indexed6438 (content) FULL_TEXT");
      database.command("sql", "INSERT INTO Indexed6438 SET id = 'a', content = 'report 2024'");
      database.command("sql", "INSERT INTO Indexed6438 SET id = 'b', content = 'no numbers here'");

      database.command("sql", "CREATE DOCUMENT TYPE Unindexed6438");
      database.command("sql", "CREATE PROPERTY Unindexed6438.content STRING");
      database.command("sql", "INSERT INTO Unindexed6438 SET id = 'a', content = 'report 2024'");
      database.command("sql", "INSERT INTO Unindexed6438 SET id = 'b', content = 'no numbers here'");
    });

    database.transaction(() -> {
      // Indexed property: the lookup already stringified the key and matched before the fix.
      assertThat(explain("SELECT id FROM Indexed6438 WHERE content CONTAINSTEXT 2024")).contains("FETCH FROM INDEX");
      assertThat(idsMatching("SELECT id FROM Indexed6438 WHERE content CONTAINSTEXT 2024")).containsExactly("a");

      // Unindexed property: before the fix ContainsTextCondition.evaluate() refused the non-String value outright
      // and matched nothing here, even though the same value against the same text matched on the indexed type.
      assertThat(idsMatching("SELECT id FROM Unindexed6438 WHERE content CONTAINSTEXT 2024")).containsExactly("a");
    });
  }

  @Test
  void numericLiteralWithNoMatchingSubstringMatchesNothingEitherWay() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Unindexed6438b");
      database.command("sql", "CREATE PROPERTY Unindexed6438b.content STRING");
      database.command("sql", "INSERT INTO Unindexed6438b SET id = 'a', content = 'report 2024'");
    });

    database.transaction(() -> assertThat(
        idsMatching("SELECT id FROM Unindexed6438b WHERE content CONTAINSTEXT 1999")).isEmpty());
  }

  private java.util.Set<String> idsMatching(final String query) {
    final java.util.Set<String> ids = new java.util.LinkedHashSet<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext())
        ids.add(rs.next().getProperty("id"));
    }
    return ids;
  }

  private String explain(final String query) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + query)) {
      return rs.next().toJSON().toString();
    }
  }
}
