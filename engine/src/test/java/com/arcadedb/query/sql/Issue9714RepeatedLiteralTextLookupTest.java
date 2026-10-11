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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.literal.LiteralParameterizer.Lookup;
import com.arcadedb.query.literal.LiteralScan;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.antlr.SQLLiteralParameterizer;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.Statement;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9714: a text whose literals were extracted was cached only under its generated key, so every repeat of the same text
 * missed the exact-text lookup and was lexed again before the shared statement was found. A repeated text must cost a lookup.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9714RepeatedLiteralTextLookupTest extends TestHelper {

  /** Counts the lexer passes, the cost the issue measured. */
  private static final class CountingParameterizer extends SQLLiteralParameterizer {
    private int scans;

    private CountingParameterizer(final SQLAntlrParser parser) {
      this(parser, 100);
    }

    private CountingParameterizer(final SQLAntlrParser parser, final int size) {
      super(parser, size);
    }

    @Override
    protected LiteralScan scan(final String text) {
      ++scans;
      return super.scan(text);
    }
  }

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE V");
    database.command("sql", "CREATE PROPERTY V.a LONG");
    database.command("sql", "CREATE INDEX ON V (a) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.newDocument("V").set("a", (long) i).save();
    });
  }

  @Test
  void aRepeatedTextIsNotLexedAgain() {
    final CountingParameterizer cache = new CountingParameterizer(new SQLAntlrParser(database));
    final String text = "SELECT count(*) AS c FROM V WHERE a = 77";

    // the first text of the shape decides its policy, the second builds the parameterized statement
    cache.lookup(text, true);
    final Lookup<Statement> second = cache.lookup(text, true);
    assertThat(second.parameters()).isEqualTo(Map.of("__lit_i0", 77));
    final int scansToBuild = cache.scans;

    for (int i = 0; i < 100; i++) {
      final Lookup<Statement> repeat = cache.lookup(text, true);
      assertThat(repeat.statement()).isSameAs(second.statement());
      assertThat(repeat.cacheKey()).isEqualTo("SELECT count(*) AS c FROM V WHERE a = :__lit_i0");
      assertThat(repeat.parameters()).isEqualTo(Map.of("__lit_i0", 77));
    }
    assertThat(cache.scans).isEqualTo(scansToBuild);

    // another value of the shape is a new text: lexed once, then remembered as well
    assertThat(cache.lookup("SELECT count(*) AS c FROM V WHERE a = 5", true).parameters()).isEqualTo(Map.of("__lit_i0", 5));
    assertThat(cache.lookup("SELECT count(*) AS c FROM V WHERE a = 5", true).parameters()).isEqualTo(Map.of("__lit_i0", 5));
    assertThat(cache.scans).isEqualTo(scansToBuild + 1);

    // with extraction off the text is cached as written, never served from the remembered extraction
    assertThat(cache.lookup("SELECT count(*) AS c FROM V WHERE a = 5", false).parameters()).isNull();

    // clear() forgets the remembered texts with the statements: the text is lexed and parsed again, never served stale
    cache.clear();
    assertThat(cache.lookup(text, true).statement()).isNotSameAs(second.statement());
    assertThat(cache.scans).isEqualTo(scansToBuild + 2);
  }

  @Test
  void aRememberedTextWhoseStatementWasEvictedIsLexedAgain() {
    // the remembered text holds no statement of its own: once its key leaves the statement cache, the text takes the lexing
    // path again and answers exactly what it answered before
    final CountingParameterizer cache = new CountingParameterizer(new SQLAntlrParser(database), 2);
    final String text = "SELECT count(*) AS c FROM V WHERE a = 77";
    cache.lookup(text, true);
    final Lookup<Statement> built = cache.lookup(text, true);
    assertThat(cache.contains(built.cacheKey())).isTrue();

    for (int i = 0; i < 10; i++)
      cache.lookup("SELECT count(*) AS c" + i + " FROM V", true);
    assertThat(cache.contains(built.cacheKey())).as("the key was evicted").isFalse();

    final int scansBefore = cache.scans;
    final Lookup<Statement> again = cache.lookup(text, true);
    assertThat(cache.scans).isEqualTo(scansBefore + 1);
    assertThat(again.cacheKey()).isEqualTo(built.cacheKey());
    assertThat(again.parameters()).isEqualTo(Map.of("__lit_i0", 77));
    assertThat(cache.contains(built.cacheKey())).isTrue();
  }

  @Test
  void eachRepeatOwnsItsValues() {
    final CountingParameterizer cache = new CountingParameterizer(new SQLAntlrParser(database));
    final String text = "SELECT count(*) AS c FROM V WHERE a = 7";
    cache.lookup(text, true);
    cache.lookup(text, true);

    // an execution may change the map it was given: the next repeat must still bind the value written in the text
    final Lookup<Statement> first = cache.lookup(text, true);
    final Map<String, Object> bound = first.mergeParameters(null);
    bound.put("__lit_i0", 8);
    final Lookup<Statement> next = cache.lookup(text, true);
    assertThat(next.parameters()).isNotSameAs(first.parameters()).isEqualTo(Map.of("__lit_i0", 7));
  }

  @Test
  void repeatedQueriesAnswerTheirOwnValues() {
    for (int round = 0; round < 3; round++)
      for (final long a : new long[] { 3, 77, 50 })
        try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM V WHERE a >= " + a)) {
          assertThat(rs.next().<Long>getProperty("c")).as("round %d, a >= %d", round, a).isEqualTo(100 - a);
        }
  }
}
