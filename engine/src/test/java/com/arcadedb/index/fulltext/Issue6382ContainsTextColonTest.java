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
package com.arcadedb.index.fulltext;

import com.arcadedb.TestHelper;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.RangeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code CONTAINSTEXT} is literal text to find, not a query language, but the direct full-text lookup split every
 * whitespace-separated part on the first {@code ':'} and looked the remainder up as a field-qualified key. A
 * single-property index never stores a qualified key, so any literal carrying a colon - a time, a timestamp, a
 * {@code ns:name} - matched nothing at all (issue #6382).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6382ContainsTextColonTest extends TestHelper {

  @Test
  void singlePropertyIndexMatchesALiteralContainingAColon() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Note6382");
      database.command("sql", "CREATE PROPERTY Note6382.label STRING");
      database.command("sql", "CREATE INDEX ON Note6382 (label) FULL_TEXT");

      database.command("sql", "INSERT INTO Note6382 SET id = 'a', label = 'foo:bar happens here'");
      database.command("sql", "INSERT INTO Note6382 SET id = 'b', label = 'unrelated text'");
      database.command("sql", "INSERT INTO Note6382 SET id = 'c', label = 'meeting at 3:30 today'");
    });

    database.transaction(() -> {
      assertThat(idsMatching("foo:bar")).containsExactly("a");
      assertThat(idsMatching("3:30")).containsExactly("c");
    });
  }

  /**
   * The Lucene-backed executor treats the sole indexed property named as a qualifier as no qualifier at all
   * ({@code FullTextQueryExecutor.isUnqualified}); the direct path has to agree.
   */
  @Test
  void singlePropertyIndexAcceptsItsOwnPropertyAsAQualifier() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Qualified6382");
      database.command("sql", "CREATE PROPERTY Qualified6382.label STRING");
      database.command("sql", "CREATE INDEX ON Qualified6382 (label) FULL_TEXT");

      database.command("sql", "INSERT INTO Qualified6382 SET id = 'a', label = 'java programming'");
      database.command("sql", "INSERT INTO Qualified6382 SET id = 'b', label = 'python scripting'");
    });

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql",
          "SELECT id FROM Qualified6382 WHERE label CONTAINSTEXT 'label:java'")) {
        assertThat(collect(rs)).containsExactly("a");
      }
    });
  }

  /**
   * A real field qualifier on a multi-property index keeps working, and a prefix that is NOT an indexed property is
   * literal text: it matches the document that really carries it, not nothing at all. Exercised through the index
   * lookup directly, because the planner does not push a single-property {@code CONTAINSTEXT} down onto a
   * multi-property full-text index.
   */
  @Test
  void multiPropertyIndexKeepsFieldQualifiersAndTreatsUnknownOnesAsLiterals() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article6382");
      database.command("sql", "CREATE PROPERTY Article6382.title STRING");
      database.command("sql", "CREATE PROPERTY Article6382.content STRING");
      database.command("sql", "CREATE INDEX ON Article6382 (title, content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article6382 SET id = 'a', title = 'java', content = 'databases'");
      database.command("sql", "INSERT INTO Article6382 SET id = 'b', title = 'python', content = 'java bindings'");
      database.command("sql", "INSERT INTO Article6382 SET id = 'c', title = 'release notes', content = 'see jira:1234 for details'");
    });

    database.transaction(() -> {
      // A real qualifier still narrows to the property it names.
      assertThat(idsFromIndex("Article6382", "title:java")).containsExactly("a");
      assertThat(idsFromIndex("Article6382", "content:java")).containsExactly("b");
      // 'jira' is not an indexed property, so the whole part stays literal text and finds the document that
      // actually contains it. Before the fix it was looked up as a qualified key nothing ever stored.
      assertThat(idsFromIndex("Article6382", "jira:1234")).containsExactly("c");
    });
  }

  private Set<String> idsFromIndex(final String typeName, final String literal) {
    final Set<String> ids = new HashSet<>();
    for (final Index index : database.getSchema().getType(typeName).getAllIndexes(true)) {
      final IndexCursor cursor = ((RangeIndex) index).get(new Object[] { literal });
      while (cursor.hasNext())
        ids.add(cursor.next().asDocument().getString("id"));
    }
    return ids;
  }

  private Set<String> idsMatching(final String literal) {
    try (final ResultSet rs = database.query("sql", "SELECT id FROM Note6382 WHERE label CONTAINSTEXT ?", literal)) {
      return collect(rs);
    }
  }

  private static Set<String> collect(final ResultSet rs) {
    final Set<String> ids = new HashSet<>();
    while (rs.hasNext())
      ids.add(rs.next().getProperty("id"));
    return ids;
  }
}
