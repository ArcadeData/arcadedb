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
package com.arcadedb.function.sql.text;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code SEARCH_INDEX} is evaluated once per row and arm of a WHERE clause. Each evaluation used to build the search object, with
 * its three cache keys, and the key of the resolved index list again, although both are a function of the index name and the query
 * only. The statement now remembers the searches it has built, so a row costs no allocation for them.
 * <p>
 * The answers are pinned against the indexed fetch (the same searches over {@code FROM Doc}), which never takes the per-row
 * path, and the allocation is counted per thread, never timed.
 */
class SearchIndexRowMemoTest extends TestHelper {
  private static final int      DOCUMENTS = 300;
  private static final String[] WORDS     = { "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota", "kappa",
      "lambda", "mu" };

  private final List<RID> all = new ArrayList<>();

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 4");
      database.command("sql", "CREATE PROPERTY Doc.title STRING");
      database.command("sql", "CREATE PROPERTY Doc.body STRING");
      database.command("sql", "CREATE INDEX ON Doc (title) FULL_TEXT");
      database.command("sql", "CREATE INDEX ON Doc (body) FULL_TEXT");
      database.command("sql", "CREATE DOCUMENT TYPE Other");
      database.command("sql", "CREATE PROPERTY Other.title STRING");
      final Random random = new Random(7);
      for (int i = 0; i < DOCUMENTS; i++) {
        final MutableDocument document = database.newDocument("Doc").set("title", text(random, 1 + random.nextInt(4)));
        if (i % 3 != 0)
          document.set("body", text(random, 1 + random.nextInt(20)));
        document.save();
        all.add(document.getIdentity());
      }
    });
  }

  private static String text(final Random random, final int length) {
    final StringBuilder text = new StringBuilder();
    for (int i = 0; i < length; i++)
      text.append(i == 0 ? "" : " ").append(WORDS[random.nextInt(WORDS.length)]);
    return text.toString();
  }

  /** Rows of a statement over {@code target} (a RID list, or the type) as RID to every score the rows carry, in order. */
  private List<Result> rows(final String target, final String condition, final Map<String, Object> params) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT @rid AS rid, $score AS score FROM " + target + " WHERE " + condition, params)) {
      rs.stream().forEach(rows::add);
    }
    return rows;
  }

  private Map<RID, Float> scores(final String target, final String condition, final Map<String, Object> params) {
    final Map<RID, Float> scores = new HashMap<>();
    for (final Result row : rows(target, condition, params))
      assertThat(scores.put(row.<RID>getProperty("rid"), row.<Float>getProperty("score"))).as("one row per record").isNull();
    return scores;
  }

  private static String list(final List<RID> rids) {
    final StringBuilder list = new StringBuilder("[");
    for (int i = 0; i < rids.size(); i++)
      list.append(i == 0 ? "" : ", ").append(rids.get(i));
    return list.append(']').toString();
  }

  /** The matches of the indexed fetch of {@code FROM Doc}, for the records of {@code target} only. */
  private Map<RID, Float> fetched(final String condition, final Map<String, Object> params, final List<RID> target) {
    final Map<RID, Float> expected = new HashMap<>(scores("Doc", condition, params));
    expected.keySet().retainAll(target);
    return expected;
  }

  @Test
  void twoArmsOverARidListScoreEachRecordWithTheHigherArm() {
    final Map<String, Object> params = Map.of("q", "alpha beta");
    final String title = "SEARCH_INDEX('Doc[title]', :q) = true";
    final String body = "SEARCH_INDEX('Doc[body]', :q) = true";

    final Map<RID, Float> titleScores = fetched(title, params, all);
    final Map<RID, Float> bodyScores = fetched(body, params, all);
    final Map<RID, Float> expected = new HashMap<>(titleScores);
    bodyScores.forEach((rid, score) -> expected.merge(rid, score, Math::max));
    assertThat(titleScores).as("the title arm matches").isNotEmpty();
    assertThat(bodyScores).as("the body arm matches").isNotEmpty();
    assertThat(expected.size()).as("some record matches one arm only").isGreaterThan(Math.max(titleScores.size(), bodyScores.size()));

    assertThat(scores(list(all), "(" + title + " OR " + body + ")", params)).isEqualTo(expected);
    assertThat(scores(list(all), "(" + body + " OR " + title + ")", params)).isEqualTo(expected);
    assertThat(scores("Doc", "(" + title + " OR " + body + ")", params)).isEqualTo(expected);
  }

  @Test
  void aRidListWithDuplicatesDeletedForeignAndNeverExistingRids() {
    final RID deleted = all.get(5);
    database.transaction(() -> deleted.asDocument().delete());
    final RID[] foreign = new RID[1];
    database.transaction(() -> {
      final MutableDocument other = database.newDocument("Other").set("title", "alpha beta alpha");
      other.save();
      foreign[0] = other.getIdentity();
    });
    final List<RID> target = new ArrayList<>(all.subList(0, 40));
    target.addAll(all.subList(10, 20));
    target.add(deleted);
    target.add(foreign[0]);
    target.add(new RID(all.getFirst().getBucketId(), 999_999L));

    final Map<String, Object> params = Map.of("q", "alpha");
    final String condition = "(SEARCH_INDEX('Doc[title]', :q) = true OR SEARCH_INDEX('Doc[body]', :q) = true)";
    final Map<RID, Float> expected = fetched(condition, params, target);
    assertThat(expected).isNotEmpty();

    final List<Result> rows = rows(list(target), condition, params);
    final Map<RID, Integer> occurrences = new HashMap<>();
    for (final RID rid : target)
      occurrences.merge(rid, 1, Integer::sum);
    final Map<RID, Integer> seen = new HashMap<>();
    for (final Result row : rows) {
      final RID rid = row.getProperty("rid");
      seen.merge(rid, 1, Integer::sum);
      assertThat(row.<Float>getProperty("score")).as(rid.toString()).isEqualTo(expected.get(rid));
    }
    assertThat(seen.keySet()).isEqualTo(expected.keySet());
    seen.forEach((rid, count) -> assertThat(count).as(rid.toString()).isEqualTo(occurrences.get(rid)));
    assertThat(seen).doesNotContainKeys(deleted, foreign[0]);
  }

  @Test
  void manyDistinctArmsOfOneStatement() {
    // more arms than a statement remembers at once: the answer is the same, whichever arms the statement keeps
    final StringBuilder condition = new StringBuilder("(");
    final Map<String, Object> params = new HashMap<>();
    for (int i = 0; i < WORDS.length; i++) {
      condition.append(i == 0 ? "" : " OR ").append("SEARCH_INDEX('Doc[title]', :q").append(i).append(") = true");
      params.put("q" + i, WORDS[i]);
    }
    condition.append(" OR SEARCH_INDEX('Doc[body]', :q0) = true)");

    final Map<RID, Float> expected = new HashMap<>();
    for (int i = 0; i < WORDS.length; i++)
      fetched("SEARCH_INDEX('Doc[title]', :q) = true", Map.of("q", WORDS[i]), all).forEach((rid, score) -> expected.merge(rid, score, Math::max));
    fetched("SEARCH_INDEX('Doc[body]', :q) = true", Map.of("q", WORDS[0]), all).forEach((rid, score) -> expected.merge(rid, score, Math::max));
    assertThat(expected).isNotEmpty();

    assertThat(scores(list(all), condition.toString(), params)).isEqualTo(expected);
  }

  @Test
  void oneArmPerQueryOfTheSameIndex() {
    final Map<String, Object> params = Map.of("a", "alpha", "b", "beta -alpha");
    final String condition = "(SEARCH_INDEX('Doc[title]', :a) = true OR SEARCH_INDEX('Doc[title]', :b) = true)";
    final Map<RID, Float> expected = new HashMap<>(fetched("SEARCH_INDEX('Doc[title]', :q) = true", Map.of("q", "alpha"), all));
    fetched("SEARCH_INDEX('Doc[title]', :q) = true", Map.of("q", "beta -alpha"), all).forEach((rid, score) -> expected.merge(rid, score, Math::max));
    assertThat(scores(list(all), condition, params)).isEqualTo(expected);
  }

  @Test
  void aSecondStatementOfTheSameTextSearchesAgain() {
    // the statement text is cached with its functions: what a statement remembers must not outlive it
    final String condition = "SEARCH_INDEX('Doc[title]', :q) = true";
    final String target = list(all);
    final Map<RID, Float> alpha = scores(target, condition, Map.of("q", "alpha"));
    final Map<RID, Float> beta = scores(target, condition, Map.of("q", "beta"));
    assertThat(alpha).isNotEmpty().isNotEqualTo(beta);
    assertThat(scores(target, condition, Map.of("q", "alpha"))).isEqualTo(alpha);

    final RID changed = all.stream().filter(rid -> !alpha.containsKey(rid)).findFirst().orElseThrow();
    database.transaction(() -> changed.asDocument().modify().set("title", "alpha alpha alpha").save());
    final Map<RID, Float> after = scores(target, condition, Map.of("q", "alpha"));
    assertThat(after).containsKey(changed);
    assertThat(after.keySet()).containsAll(alpha.keySet());
    assertThat(after).isEqualTo(fetched(condition, Map.of("q", "alpha"), all));
  }

  @Test
  void anEmptyQueryIsRefusedOverARidListAndTheType() {
    final String condition = "SEARCH_INDEX('Doc[title]', :q) = true";
    final Map<String, Object> params = Map.of("q", "");
    assertThatThrownBy(() -> rows(list(all), condition, params)).hasMessageContaining("Invalid search query");
    assertThatThrownBy(() -> rows("Doc", condition, params)).hasMessageContaining("Invalid search query");
    assertThatThrownBy(() -> rows(list(all), "(" + condition + " OR SEARCH_INDEX('Doc[body]', :q) = true)", params))
        .hasMessageContaining("Invalid search query");
  }

  @Test
  void aRowInsertedByTheOpenTransactionIsFoundByTheNextStatement() {
    final String condition = "(SEARCH_INDEX('Doc[title]', :q) = true OR SEARCH_INDEX('Doc[body]', :q) = true)";
    final Map<String, Object> params = Map.of("q", "zzzmarker");
    database.begin();
    try {
      final List<RID> target = new ArrayList<>(all.subList(0, 30));
      assertThat(rows(list(target), condition, params)).isEmpty();

      final MutableDocument inserted = database.newDocument("Doc").set("title", "zzzmarker one").set("body", "zzzmarker two");
      inserted.save();
      target.add(inserted.getIdentity());
      target.add(inserted.getIdentity());
      final Map<RID, Float> expected = fetched(condition, params, target);
      assertThat(expected).containsOnlyKeys(inserted.getIdentity());

      final List<Result> rows = rows(list(target), condition, params);
      assertThat(rows).hasSize(2);
      for (final Result row : rows) {
        assertThat(row.<RID>getProperty("rid")).isEqualTo(inserted.getIdentity());
        assertThat(row.<Float>getProperty("score")).isEqualTo(expected.get(inserted.getIdentity()));
      }
    } finally {
      database.rollback();
    }
    assertThat(rows(list(all.subList(0, 30)), condition, params)).isEmpty();
  }

  @Test
  void aRecordChangedWhileTheStatementIsBeingPulledStillAnswersEveryOtherRecord() {
    // a record changes between two pulls of one statement: the corpus statistics move, so a score may differ in its digits from the
    // statement without the change, but the rows are the records that matched, plus the changed one if the pull reaches it after
    // the change
    final String condition = "(SEARCH_INDEX('Doc[title]', :q) = true OR SEARCH_INDEX('Doc[body]', :q) = true)";
    final Map<String, Object> params = Map.of("q", "alpha");
    final List<RID> target = new ArrayList<>(all.subList(0, 60));
    final Map<RID, Float> before = fetched(condition, params, target);
    final RID late = target.stream().filter(rid -> !before.containsKey(rid)).skip(3).findFirst().orElseThrow();

    final Map<RID, Float> seen = new HashMap<>();
    database.begin();
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid, $score AS score FROM " + list(target) + " WHERE " + condition,
        params)) {
      final Result first = rs.next();
      seen.put(first.getProperty("rid"), first.getProperty("score"));
      late.asDocument().modify().set("title", "alpha alpha").save();
      while (rs.hasNext()) {
        final Result row = rs.next();
        seen.put(row.getProperty("rid"), row.getProperty("score"));
      }
    } finally {
      database.rollback();
    }
    // every record that matched before the change still matches, and no other record than the changed one appears
    assertThat(seen.keySet()).containsAll(before.keySet());
    final List<RID> allowed = new ArrayList<>(before.keySet());
    allowed.add(late);
    assertThat(seen.keySet()).isSubsetOf(allowed);
  }

  /** The cost of the condition of one row, per call, with the matches of the query cached by an earlier row. */
  @Test
  void aRowOfTheTargetDoesNotBuildItsSearchAgain() {
    final ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    assertThat(threads.isThreadAllocatedMemorySupported() && threads.isThreadAllocatedMemoryEnabled())
        .as("the JVM counts the bytes a thread allocates").isTrue();

    final SQLFunctionSearchIndex function = new SQLFunctionSearchIndex();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    // a wildcard is answered by the complete search, cached for the statement after the first row: what is left per row is the
    // evaluation itself, which is what is counted
    final Object[] arguments = { "Doc[title]", "alph*" };
    final Document record = all.getFirst().asDocument();
    final long threadId = Thread.currentThread().threadId();

    for (int i = 0; i < 5_000; i++)
      function.execute(null, record, null, arguments, context);
    final int calls = 20_000;
    final long before = threads.getThreadAllocatedBytes(threadId);
    for (int i = 0; i < calls; i++)
      function.execute(null, record, null, arguments, context);
    final long perCall = (threads.getThreadAllocatedBytes(threadId) - before) / calls;

    // The three keys, the members key and the search object cost 408 bytes per evaluation without the memo (JDK 21, HotSpot, the
    // extended ThreadMXBean counts exactly per thread). The bound is under a third of that: what is left per row is the lookup and
    // the boxed score, and the figure does not depend on timing or load.
    assertThat(perCall).as("bytes allocated per evaluation").isLessThan(120L);
  }
}
