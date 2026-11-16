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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.NamedParameter;
import com.arcadedb.query.sql.parser.Statement;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests, one nested class per reported case.
 */
class SqlRepeatedLiteralTextTest {
  /**
   * Regression for #9714.
   * <p>
   * Issue #9714: a text whose literals were extracted was cached only under its generated key, so every repeat of the same text
   * missed the exact-text lookup and was lexed again before the shared statement was found. A repeated text must cost a lookup.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RepeatedLiteralTextLookup extends TestHelper {

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

  /**
   * Regression for #9818.
   * <p>
   * Issue #9818: a repeated SQL text cost 13 to 18 percent more per call once its literals were extracted (#8307). The lexing of
   * every repeat is gone since #9714; what stayed on the hit path was three lock acquisitions, a scan of the text for the
   * generated-name prefix, and a named parameter that built {@code ":" + name} and probed the parameter map three times each time
   * it was read, for every extracted literal. These tests pin what the cheaper paths must keep answering exactly as before.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RepeatedLiteralTextCost extends TestHelper {

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

    private static NamedParameter named(final String name) {
      final NamedParameter parameter = new NamedParameter();
      parameter.paramName = name;
      parameter.paramNumber = 3;
      return parameter;
    }

    @Test
    void aNamedParameterReadsItsColonPrefixedKeyBeforeItsPlainKey() {
      final Map<String, Object> params = new HashMap<>();
      params.put("p", "plain");
      assertThat(named("p").getValue(params)).isEqualTo("plain");

      params.put(":p", "prefixed");
      assertThat(named("p").getValue(params)).isEqualTo("prefixed");
    }

    @Test
    void aNamedParameterBoundToNullUnderEitherKeyAnswersNull() {
      final Map<String, Object> prefixedNull = new HashMap<>();
      prefixedNull.put(":p", null);
      prefixedNull.put("p", "plain");
      assertThat(named("p").getValue(prefixedNull)).as("an explicit null under the prefixed key stops the search").isNull();

      final Map<String, Object> plainNull = new HashMap<>();
      plainNull.put("p", null);
      plainNull.put("3", "positional");
      assertThat(named("p").getValue(plainNull)).as("an explicit null under the plain key stops the search").isNull();
    }

    @Test
    void aNamedParameterFallsBackToItsPositionAndToNothing() {
      final Map<String, Object> positional = new HashMap<>();
      positional.put("3", "third");
      assertThat(named("p").getValue(positional)).isEqualTo("third");

      assertThat(named("p").getValue(new HashMap<>())).isNull();
      assertThat(named("p").getValue(null)).isNull();
    }

    @Test
    void bindingANamedParameterKeepsItsKeyBoundToNullAndItsFallbacks() {
      final Map<String, Object> plainNull = new HashMap<>();
      plainNull.put("p", null);
      plainNull.put("3", 7);
      assertThat(named("p").bindFromInputParams(plainNull)).as("a key bound to null binds null, not the position").isInstanceOf(Expression.class);
      assertThat(((Expression) named("p").bindFromInputParams(plainNull)).execute((Result) null, null)).isNull();

      final Map<String, Object> plain = new HashMap<>();
      plain.put("p", 5);
      plain.put("3", 7);
      assertThat(named("p").bindFromInputParams(plain)).hasToString("5");

      final Map<String, Object> positional = new HashMap<>();
      positional.put("3", 7);
      assertThat(named("p").bindFromInputParams(positional)).hasToString("7");

      final NamedParameter unbound = named("p");
      assertThat(unbound.bindFromInputParams(null)).as("without parameters the parameter stays itself").isSameAs(unbound);
    }

    @Test
    void aNamedParameterFollowsARenamedField() {
      // paramName is a public field: a name changed after the first read must be the one the next read looks up
      final NamedParameter parameter = named("first");
      final Map<String, Object> params = Map.of("first", 1, "second", 2);
      assertThat(parameter.getValue(params)).isEqualTo(1);
      parameter.paramName = "second";
      assertThat(parameter.getValue(params)).isEqualTo(2);
      assertThat(parameter.copy().getValue(params)).isEqualTo(2);
    }

    @Test
    void aRepeatedTextKeepsAnsweringItsOwnValuesWhileOthersHitEvictAndClear() throws Exception {
      // the remembered-text cache and the statement cache are read and written under one lock on the hit path: texts hit from
      // many threads, a cache too small to hold them (constant eviction) and a concurrent clear() must never hand a thread
      // another text's values, a statement of another shape, or an exception
      final int threads = 8;
      final int texts = 12;
      final SQLLiteralParameterizer cache = new SQLLiteralParameterizer(new SQLAntlrParser(database), 6);
      final AtomicInteger wrong = new AtomicInteger();
      final CountDownLatch go = new CountDownLatch(1);
      final ExecutorService pool = Executors.newFixedThreadPool(threads + 1);
      try {
        final Future<?>[] futures = new Future<?>[threads + 1];
        for (int t = 0; t < threads; t++) {
          final int seed = t;
          futures[t] = pool.submit(() -> {
            go.await();
            for (int i = 0; i < 4_000; i++) {
              final int value = (seed + i) % texts;
              final String text = "SELECT count(*) AS c FROM V WHERE a = " + value;
              final Lookup<Statement> lookup = cache.lookup(text, true);
              if (lookup.parameters() != null) {
                // extracted: the one value is the one written, under a key of the shape
                if (!lookup.cacheKey().equals("SELECT count(*) AS c FROM V WHERE a = :__lit_i0")
                    || !Integer.valueOf(value).equals(lookup.parameters().get("__lit_i0")))
                  wrong.incrementAndGet();
              } else if (!lookup.cacheKey().equals(text))
                // as written: keyed by its own text
                wrong.incrementAndGet();
            }
            return null;
          });
        }
        futures[threads] = pool.submit(() -> {
          go.await();
          for (int i = 0; i < 400; i++) {
            cache.clear();
            // a yield rather than a timed sleep, whose granularity depends on the platform: clear() lands between the hits at an
            // arbitrary point on every OS, which is all this stress needs
            Thread.yield();
          }
          return null;
        });
        go.countDown();
        for (final Future<?> future : futures)
          future.get(60, TimeUnit.SECONDS);
      } finally {
        pool.shutdownNow();
      }
      assertThat(wrong.get()).isZero();
    }

    @Test
    void aRepeatedTextExecutesToTheSameAnswerAsItsFirstRun() {
      // end to end through the engine: the same text over and over, interleaved with other values of its shape
      for (int round = 0; round < 5; round++)
        for (int value = 0; value < 20; value++)
          try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM V WHERE a = " + value)) {
            assertThat(rs.next().<Long>getProperty("c")).isEqualTo(1L);
          }
      try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM V WHERE a = 1000")) {
        assertThat(rs.next().<Long>getProperty("c")).isZero();
      }
    }
  }
}
