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
package com.arcadedb.function.sql.facet;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarSnapshot;
import com.arcadedb.index.columnar.ColumnarTestHooks;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code facet.searchValues(type, property, prefix, limit)}: the values of a COLUMNAR dictionary property starting with a prefix, in
 * value order, each with the number of live records of the type carrying it now. The per-bucket dictionaries keep the values of deleted
 * and updated records until compaction, have no access check and are searched only for the buckets the user may read, so every
 * candidate is counted through FACETS and a value with no live readable record is never returned.
 */
class SQLFunctionFacetSearchValuesTest extends TestHelper {
  private static final String TYPE    = "Asset";
  /** A hang detector, never a latency bound. */
  private static final long   HANG_MS = 60_000L;

  private void createSchema(final int buckets, final String subtype) {
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE + " BUCKETS " + buckets);
    database.command("sql", "CREATE PROPERTY " + TYPE + ".tags LIST OF STRING");
    database.command("sql", "CREATE PROPERTY " + TYPE + ".brand STRING");
    if (subtype != null)
      database.command("sql", "CREATE DOCUMENT TYPE " + subtype + " EXTENDS " + TYPE);
    database.command("sql", "CREATE INDEX ON " + TYPE + " (tags BY ITEM) COLUMNAR");
    FacetOracle.awaitCaughtUp(database);
  }

  private void insert(final String type, final String... tags) {
    database.newDocument(type).set("tags", List.of(tags)).set("brand", "b").save();
  }

  /** run x3, runner x1, running x1, rum x2, other x1; runaway only on a deleted record. */
  private void load(final int buckets) {
    createSchema(buckets, null);
    database.transaction(() -> {
      insert(TYPE, "run", "rum");
      insert(TYPE, "runner");
      insert(TYPE, "running", "run");
      insert(TYPE, "rum");
      insert(TYPE, "run");
      insert(TYPE, "other");
    });
    final RID[] runaway = new RID[1];
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument(TYPE).set("tags", List.of("runaway")).set("brand", "b");
      doc.save();
      runaway[0] = doc.getIdentity();
    });
    FacetOracle.awaitCaughtUp(database);
    database.transaction(() -> database.deleteRecord(runaway[0].asDocument()));
    FacetOracle.awaitCaughtUp(database);
  }

  private List<Result> search(final String type, final String property, final String prefix, final int limit) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT expand(facet.searchValues(:t, :p, :x, :l))",
        Map.of("t", type, "p", property, "x", prefix, "l", limit))) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }

  private static List<Object> values(final List<Result> rows) {
    final List<Object> values = new ArrayList<>(rows.size());
    for (final Result row : rows)
      values.add(row.getProperty("value"));
    return values;
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  /** Whether the dictionary of some bucket of the type holds the value: what makes a test of the deleted-value filter able to fail. */
  private boolean someDictionaryHolds(final String type, final String value) {
    for (final Bucket bucket : database.getSchema().getType(type).getBuckets(true))
      try (final ColumnarSnapshot snapshot = manager().projection(bucket.getFileId()).acquire()) {
        if (snapshot.dictionary("tags").ordinalOf(value) >= 0)
          return true;
      }
    return false;
  }

  @Test
  @Timeout(300)
  void isRegistered() {
    assertThat(DefaultSQLFunctionFactory.getInstance().getFunctionNames()).contains(SQLFunctionFacetSearchValues.NAME.toLowerCase(Locale.ENGLISH));
  }

  @Test
  @Timeout(300)
  void returnsLiveValuesWithCountsInValueOrder() {
    load(1);
    assertThat(someDictionaryHolds(TYPE, "runaway")).as("the dictionary keeps the deleted record's value").isTrue();

    final List<Result> rows = toList(database.query("sql", "SELECT expand(facet.searchValues('Asset', 'tags', 'run', 20))"));
    assertThat(values(rows)).containsExactly("run", "runner", "running");
    final Map<Object, Long> expected = FacetOracle.expectedCounts(database, TYPE, "tags", null);
    for (final Result row : rows)
      assertThat(row.<Long>getProperty("count")).isEqualTo(expected.get(row.<String>getProperty("value")));
    assertThat(rows.get(0).<Long>getProperty("count")).isEqualTo(3L);
  }

  private static List<Result> toList(final ResultSet rs) {
    final List<Result> rows = new ArrayList<>();
    try (rs) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }

  @Test
  @Timeout(300)
  void limitStopsAtLimitLiveValues() {
    load(1);
    assertThat(values(search(TYPE, "tags", "run", 2))).containsExactly("run", "runner");
    assertThat(values(search(TYPE, "tags", "run", 1))).containsExactly("run");
  }

  @Test
  @Timeout(300)
  void edgeInputs() {
    load(1);
    // the empty prefix matches every value: the first ones in value order
    assertThat(values(search(TYPE, "tags", "", 2))).containsExactly("other", "rum");
    assertThat(values(search(TYPE, "tags", "", 20))).containsExactly("other", "rum", "run", "runner", "running");
    // a prefix is literal, never a pattern
    assertThat(search(TYPE, "tags", "a.*", 20)).isEmpty();
    assertThat(search(TYPE, "tags", "r.*", 20)).isEmpty();
    assertThat(search(TYPE, "tags", "run", 0)).isEmpty();
    assertThatThrownBy(() -> search(TYPE, "tags", "run", -1)).isInstanceOf(IllegalArgumentException.class)
        .hasMessage("facet.searchValues: limit must be >= 0");
    assertThatThrownBy(() -> search("Nope", "tags", "run", 5)).isInstanceOf(CommandExecutionException.class).hasMessageContaining("Nope");
    assertThatThrownBy(() -> search(TYPE, "brand", "b", 5)).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("no COLUMNAR index on Asset.brand");
    assertThatThrownBy(() -> search(TYPE, "nope", "b", 5)).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("no COLUMNAR index on Asset.nope");
    // a name is never concatenated into the counting query unchecked
    assertThatThrownBy(() -> search("Asset FACETS x: tags TERMS", "tags", "run", 5)).isInstanceOf(CommandExecutionException.class);
  }

  @Test
  @Timeout(300)
  void limitIsCappedByTopMax() {
    createSchema(1, null);
    database.transaction(() -> {
      for (int i = 0; i < 12; i++)
        insert(TYPE, String.format("v%02d", i));
    });
    FacetOracle.awaitCaughtUp(database);
    final Object previous = GlobalConfiguration.COLUMNAR_TOP_MAX.getValue();
    GlobalConfiguration.COLUMNAR_TOP_MAX.setValue(5);
    try {
      assertThat(values(search(TYPE, "tags", "v", 5000))).containsExactly("v00", "v01", "v02", "v03", "v04");
    } finally {
      GlobalConfiguration.COLUMNAR_TOP_MAX.setValue(previous);
    }
    assertThat(search(TYPE, "tags", "v", 5000)).hasSize(12);
  }

  /**
   * The hidden subtype holds 200 values sorting before {@code run}: searched, they would fill the 8 batches of 16 candidates (limit 1)
   * with values the user cannot count and the answer would be empty. The counts exclude its records too.
   */
  @Test
  @Timeout(300)
  void respectsBucketSecurity() {
    createSchema(1, "Photo");
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        insert(TYPE, "run");
      insert(TYPE, "runner");
      for (int i = 0; i < 200; i++)
        insert("Photo", String.format("ru%03d", i));
      insert("Photo", "run");
      insert("Photo", "run");
    });
    FacetOracle.awaitCaughtUp(database);

    // a user who may read everything sees the subtype's values and records
    assertThat(values(search(TYPE, "tags", "ru", 1))).containsExactly("ru000");
    final List<Result> all = search(TYPE, "tags", "run", 5);
    assertThat(values(all)).containsExactly("run", "runner");
    assertThat(all.get(0).<Long>getProperty("count")).isEqualTo(5L);

    final Set<Integer> hidden = new HashSet<>(database.getSchema().getType("Photo").getBucketIds(false));
    final DatabaseContext.DatabaseContextTL context = DatabaseContext.INSTANCE.getContext(database.getDatabasePath());
    context.setCurrentUser(userWithoutReadOn(hidden));
    try {
      final List<Result> rows = search(TYPE, "tags", "ru", 1);
      assertThat(values(rows)).containsExactly("run");
      assertThat(rows.get(0).<Long>getProperty("count")).isEqualTo(3L);
      final List<Result> restricted = search(TYPE, "tags", "run", 5);
      assertThat(values(restricted)).containsExactly("run", "runner");
      assertThat(restricted.get(0).<Long>getProperty("count")).isEqualTo(3L);
      assertThat(search(TYPE, "tags", "ru0", 5)).isEmpty();
    } finally {
      context.setCurrentUser(null);
    }
  }

  @Test
  @Timeout(300)
  void multiBucketValuesAreMergedOnce() {
    createSchema(3, null);
    database.transaction(() -> {
      for (int i = 0; i < 30; i++)
        insert(TYPE, i % 2 == 0 ? "run" : "runner", i % 5 == 0 ? "running" : "rum", "x" + i);
    });
    FacetOracle.awaitCaughtUp(database);
    final Set<Integer> runBuckets = new HashSet<>();
    final Set<Integer> runnerBuckets = new HashSet<>();
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS r, tags FROM " + TYPE)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final List<String> tags = row.getProperty("tags");
        final int bucketId = row.<RID>getProperty("r").getBucketId();
        if (tags.contains("run"))
          runBuckets.add(bucketId);
        if (tags.contains("runner"))
          runnerBuckets.add(bucketId);
      }
    }
    assertThat(runBuckets).hasSizeGreaterThan(1);
    assertThat(runnerBuckets).hasSizeGreaterThan(1);

    final List<Result> rows = search(TYPE, "tags", "ru", 20);
    assertThat(values(rows)).containsExactly("rum", "run", "runner", "running");
    final Map<Object, Long> expected = FacetOracle.expectedCounts(database, TYPE, "tags", null);
    for (final Result row : rows)
      assertThat(row.<Long>getProperty("count")).isEqualTo(expected.get(row.<String>getProperty("value")));
    // a small limit takes values across the buckets in value order
    assertThat(values(search(TYPE, "tags", "ru", 2))).containsExactly("rum", "run");
  }

  /**
   * Values whose records are gone (deleted, or updated to other tags) stay in the dictionary until compaction: INCLUDE counts them 0
   * and they are dropped. 20 of them sort before {@code run}, more than one batch of 16 for limit 1: the search goes on to the next.
   */
  @Test
  @Timeout(300)
  void zeroCountCandidatesAreDropped() {
    load(1);
    final List<RID> gone = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 20; i++) {
        final MutableDocument doc = database.newDocument(TYPE).set("tags", List.of(String.format("ru%02d", i))).set("brand", "b");
        doc.save();
        gone.add(doc.getIdentity());
      }
    });
    FacetOracle.awaitCaughtUp(database);
    database.transaction(() -> {
      for (int i = 0; i < gone.size(); i++)
        if (i % 2 == 0)
          database.deleteRecord(gone.get(i).asDocument());
        else
          gone.get(i).asDocument().modify().set("tags", List.of("other")).save();
    });
    FacetOracle.awaitCaughtUp(database);
    assertThat(someDictionaryHolds(TYPE, "ru00")).isTrue();
    assertThat(someDictionaryHolds(TYPE, "ru19")).isTrue();
    assertThat(someDictionaryHolds(TYPE, "runaway")).isTrue();

    final List<Result> rows = search(TYPE, "tags", "ru", 1);
    assertThat(values(rows)).containsExactly("rum");
    assertThat(values(search(TYPE, "tags", "ru", 20))).containsExactly("rum", "run", "runner", "running");
    assertThat(search(TYPE, "tags", "runa", 20)).isEmpty();
    assertThat(search(TYPE, "tags", "ru0", 20)).isEmpty();
  }

  @Test
  @Timeout(300)
  void projectionNotReadyFailsWithRetry() throws Exception {
    load(1);
    database.command("sql", "DROP INDEX `" + TYPE + "[tagsbyitem]_columnar`");
    FacetOracle.awaitCaughtUp(database);
    final String bucketName = database.getSchema().getType(TYPE).getBuckets(false).getFirst().getName();

    final CountDownLatch reached = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    ColumnarTestHooks.killPoint = name -> {
      if ("rebuild.scanned".equals(name)) {
        reached.countDown();
        try {
          release.await(HANG_MS, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    };
    try {
      database.command("sql", "CREATE INDEX ON " + TYPE + " (tags BY ITEM) COLUMNAR");
      assertThat(reached.await(HANG_MS, TimeUnit.MILLISECONDS)).as("the create rebuild reached the end of its scan").isTrue();
      assertThatThrownBy(() -> search(TYPE, "tags", "run", 5)).isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("facet.searchValues: the projection of bucket " + bucketName + " is not ready, retry");
    } finally {
      ColumnarTestHooks.killPoint = null;
      release.countDown();
    }
    FacetOracle.awaitCaughtUp(database);
    assertThat(values(search(TYPE, "tags", "run", 5))).containsExactly("run", "runner", "running");
  }

  /** The buffers and the batch are charged before they are allocated: a budget too small for them fails the call, nothing is half answered. */
  @Test
  @Timeout(300)
  void memoryBudgetRefusalFailsWithRetry() {
    load(1);
    final Object budget = GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.getValue();
    final Object wait = GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.getValue();
    GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(1024L);
    GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.setValue(0L);
    try {
      assertThatThrownBy(() -> search(TYPE, "tags", "run", 5)).isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("facet.searchValues: the memory budget refused").hasMessageContaining("retry");
    } finally {
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(budget);
      GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.setValue(wait);
    }
    assertThat(values(search(TYPE, "tags", "run", 5))).containsExactly("run", "runner", "running");
  }

  /**
   * Spec 7.3: counts the internal FACETS query could not patch (the patch budget exceeded while apply lags) are not exact, and say so:
   * every row of such a batch carries {@code exact: false}. Exact rows keep the brief's shape {@code {value, count}}.
   */
  @Test
  @Timeout(300)
  void inexactCountsAreMarked() throws Exception {
    load(1);
    final List<Result> exact = search(TYPE, "tags", "run", 5);
    assertThat(values(exact)).containsExactly("run", "runner", "running");
    for (final Result row : exact)
      assertThat(row.hasProperty("exact")).isFalse();

    final Object budget = GlobalConfiguration.COLUMNAR_PATCH_BUDGET.getValue();
    final Object ratio = GlobalConfiguration.COLUMNAR_PATCH_BUDGET_RATIO.getValue();
    GlobalConfiguration.COLUMNAR_PATCH_BUDGET.setValue(0);
    GlobalConfiguration.COLUMNAR_PATCH_BUDGET_RATIO.setValue(0d);
    try (final AutoCloseable hold = manager().suspendApply()) {
      // runner leaves its only record: the projection, not applied, still counts it
      database.transaction(() -> {
        try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE + " WHERE tags CONTAINS 'runner'")) {
          rs.next().getElement().get().asDocument().modify().set("tags", List.of("other")).save();
        }
      });
      final List<Result> rows = search(TYPE, "tags", "run", 5);
      assertThat(values(rows)).containsExactly("run", "runner", "running");
      for (final Result row : rows)
        assertThat(row.<Boolean>getProperty("exact")).as("row " + row.getProperty("value")).isFalse();
    } finally {
      GlobalConfiguration.COLUMNAR_PATCH_BUDGET.setValue(budget);
      GlobalConfiguration.COLUMNAR_PATCH_BUDGET_RATIO.setValue(ratio);
    }
    FacetOracle.awaitCaughtUp(database);
    final List<Result> caughtUp = search(TYPE, "tags", "run", 5);
    assertThat(values(caughtUp)).containsExactly("run", "running");
    for (final Result row : caughtUp)
      assertThat(row.hasProperty("exact")).isFalse();
  }

  /** A user who may do everything but read the records of {@code hidden}. */
  private static SecurityDatabaseUser userWithoutReadOn(final Set<Integer> hidden) {
    return new SecurityDatabaseUser() {
      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return access != ACCESS.READ_RECORD || !hidden.contains(fileId);
      }

      @Override
      public String getName() {
        return "restricted";
      }

      @Override
      public long getResultSetLimit() {
        return -1L;
      }

      @Override
      public long getReadTimeout() {
        return -1L;
      }
    };
  }
}
