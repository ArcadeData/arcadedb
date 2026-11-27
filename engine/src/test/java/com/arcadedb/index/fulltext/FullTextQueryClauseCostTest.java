/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.database.Identifiable;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The regexp and wildcard clauses of a full-text query are matched against the index keys with {@code java.util.regex} under the
 * shared regex deadline, so what they cost to parse must not depend on what they would cost to compile into an automaton: a
 * repeat count or a wildcard shape the automaton compiler finds expensive is parsed as cheaply as any other clause, and the answer
 * is the one the matcher gives.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class FullTextQueryClauseCostTest extends TestHelper {

  /** A repeat count the automaton compiler would expand into that many copies of its operand. */
  private static final int LARGE_COUNT = 100_000;

  /** What parsing and running a clause may allocate, by far less than building the automaton of {@link #LARGE_COUNT} copies. */
  private static final long ALLOCATION_BOUND = 1024L * 1024L;

  @Test
  void aRegexpClauseWithALargeRepeatCountAllocatesLittle() {
    createDocuments("BUCKETS 1");

    database.transaction(() -> {
      final LSMTreeFullTextIndex ftIndex = singleBucketIndex();

      assertThat(matches(ftIndex, "/a{" + LARGE_COUNT + "}/")).isEmpty();
      assertThat(allocatedBytes(() -> matches(ftIndex, "/a{" + LARGE_COUNT + "}/")))
          .as("bytes allocated by a regexp clause with a repeat count of %d", LARGE_COUNT).isLessThan(ALLOCATION_BOUND);
      assertThat(allocatedBytes(() -> matches(ftIndex, "/a{1," + LARGE_COUNT + "}b/")))
          .as("bytes allocated by a regexp clause with a repeat range up to %d", LARGE_COUNT).isLessThan(ALLOCATION_BOUND);
    });
  }

  @Test
  void aRegexpClauseWithARepeatCountBeyondTheIntegerRangeMatchesNothing() {
    createDocuments("BUCKETS 1");

    database.transaction(() -> {
      final LSMTreeFullTextIndex ftIndex = singleBucketIndex();

      assertThat(matches(ftIndex, "/a{99999999999}/")).isEmpty();
      assertThat(matches(ftIndex, "/a{1,99999999999}/")).isEmpty();
      assertThat(matches(ftIndex, "/a{99999999999,1}b/")).isEmpty();
    });
  }

  @Test
  void aWildcardClauseOfAShapeCostlyToCompileIsAnsweredLikeAnyOther() {
    createDocuments("BUCKETS 1");

    database.transaction(() -> {
      final LSMTreeFullTextIndex ftIndex = singleBucketIndex();

      // The stored token is x, q, a and then fifteen more characters: the wildcard reads as the same, one '*' and fifteen '?'
      assertThat(matches(ftIndex, "x*a" + "?".repeat(15))).hasSize(1);
      assertThat(matches(ftIndex, "x*a" + "?".repeat(16))).isEmpty();
      assertThat(allocatedBytes(() -> matches(ftIndex, "x*a" + "?".repeat(15)))).as("bytes allocated by a wildcard clause")
          .isLessThan(ALLOCATION_BOUND);
    });
  }

  @Test
  void regexpAndWildcardClausesKeepMatching() {
    createDocuments("BUCKETS 1");

    database.transaction(() -> {
      final LSMTreeFullTextIndex ftIndex = singleBucketIndex();

      assertThat(matches(ftIndex, "/a{3}b/")).hasSize(1);
      assertThat(matches(ftIndex, "/a{4}b/")).isEmpty();
      assertThat(matches(ftIndex, "/a{1,5}b/")).hasSize(1);
      assertThat(matches(ftIndex, "/aaa[b-c]/")).hasSize(1);
      assertThat(matches(ftIndex, "/xq.*/")).hasSize(1);
      assertThat(matches(ftIndex, "aa?b")).hasSize(1);
      assertThat(matches(ftIndex, "aa*")).hasSize(1);
      assertThat(matches(ftIndex, "a*b")).hasSize(1);
      assertThat(matches(ftIndex, "+/a{3}b/ -/xq.*/")).isEmpty();
      assertThat(matches(ftIndex, "/a{3}b/^2 OR zzz")).hasSize(1);
    });
  }

  @Test
  void regexpAndWildcardClausesOfAMultiBucketTypeAreParsedCheaply() {
    createDocuments("BUCKETS 3");

    assertThat(count("SELECT FROM Doc WHERE SEARCH_INDEX('Doc_ft', '/a{" + LARGE_COUNT + "}/') = true")).isZero();
    assertThat(count("SELECT FROM Doc WHERE SEARCH_INDEX('Doc_ft', 'x*a" + "?".repeat(15) + "') = true")).isEqualTo(1);
    assertThat(count("SELECT FROM Doc WHERE SEARCH_INDEX('Doc_ft', '/a{3}b/') = true")).isEqualTo(1);
    assertThat(count("SELECT FROM Doc WHERE SEARCH_FIELDS(['title'], '/a{" + LARGE_COUNT + "}/') = true")).isZero();
    assertThat(count("SELECT FROM Doc WHERE SEARCH_FIELDS(['title', 'content'], '/a{3}b/') = true")).isEqualTo(1);
    assertThat(count("SELECT FROM Doc WHERE SEARCH_FIELDS(['content'], 'x*a" + "?".repeat(15) + "') = true")).isEqualTo(1);
    assertThat(count("SELECT FROM Doc WHERE SEARCH_FIELDS(['title'], 'x*a" + "?".repeat(15) + "') = true")).isZero();
  }

  /** A regexp clause of a restricted search is expanded over its fields by the parser: each of them is as cheap as the one. */
  @Test
  void aRestrictedSearchParsesALargeRepeatCountCheaply() {
    createDocuments("BUCKETS 1");

    database.transaction(() -> {
      final LSMTreeFullTextIndex ftIndex = singleBucketIndex();
      final List<String> fields = List.of("title", "content");

      assertThat(matches(ftIndex, fields, "/a{" + LARGE_COUNT + "}/")).isEmpty();
      assertThat(allocatedBytes(() -> matches(ftIndex, fields, "/a{" + LARGE_COUNT + "}/"))).isLessThan(ALLOCATION_BOUND);
      assertThat(matches(ftIndex, fields, "/a{3}b/")).hasSize(1);
    });
  }

  private void createDocuments(final String buckets) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc " + buckets);
      database.command("sql", "CREATE PROPERTY Doc.title STRING");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX Doc_ft ON Doc (title, content) FULL_TEXT");

      database.command("sql", "INSERT INTO Doc SET title = 'aaab', content = 'xqaabcdefghijklmno'");
      database.command("sql", "INSERT INTO Doc SET title = 'second', content = 'nothing to see'");
    });
  }

  private LSMTreeFullTextIndex singleBucketIndex() {
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Doc_ft");
    return (LSMTreeFullTextIndex) index.getIndexesOnBuckets()[0];
  }

  private List<Identifiable> matches(final LSMTreeFullTextIndex ftIndex, final String query) {
    return matches(ftIndex, null, query);
  }

  private List<Identifiable> matches(final LSMTreeFullTextIndex ftIndex, final List<String> restrictedFields, final String query) {
    final FullTextQueryExecutor executor = new FullTextQueryExecutor(ftIndex, WorkGuard.forCommandDeadline(null), restrictedFields,
        null);
    final IndexCursor cursor = executor.search(query, -1);
    final List<Identifiable> matches = new ArrayList<>();
    while (cursor.hasNext())
      matches.add(cursor.next());
    return matches;
  }

  private int count(final String sql) {
    final List<Object> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows.size();
  }

  /** The least a few runs of the action allocate on this thread, so a compilation in the middle of one does not count. */
  private static long allocatedBytes(final Runnable action) {
    final ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    // THE THREAD'S ALLOCATION COUNTER IS A HOTSPOT FEATURE: A JVM WITHOUT IT HAS NOTHING TO MEASURE
    assumeTrue(threads.isThreadAllocatedMemorySupported() && threads.isThreadAllocatedMemoryEnabled());
    final long thread = Thread.currentThread().threadId();

    long least = Long.MAX_VALUE;
    for (int round = 0; round < 3; round++) {
      final long before = threads.getThreadAllocatedBytes(thread);
      action.run();
      least = Math.min(least, threads.getThreadAllocatedBytes(thread) - before);
    }
    return least;
  }
}
