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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.event.BeforeRecordUpdateListener;
import com.arcadedb.index.Index;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code ALTER MATERIALIZED VIEW ... REFRESH INCREMENTAL} is the upgrade path of a per-record view (issue #11475): it gives an
 * existing view the source link, keeps it across a mode change and a source rename, and does not lose what a transaction open
 * across the ALTER captured.
 */
class MaterializedViewPerRecordAlterTest extends TestHelper {

  private static final String ADULTS_QUERY = "SELECT name, age FROM Person WHERE age >= 18";

  @Test
  void manualViewAlteredToIncrementalIsUpgraded() {
    createPersons(200);
    final MaterializedViewImpl manual = createView("Adults", ADULTS_QUERY, MaterializedViewRefreshMode.MANUAL);
    assertThat(manual.isPerRecord()).isFalse();
    assertThat(database.getSchema().getType("Adults").existsProperty(MaterializedViewPerRecordRefresher.SOURCE_RID)).isFalse();

    alter("Adults", "INCREMENTAL");

    final MaterializedViewImpl view = view("Adults");
    assertThat(view.isPerRecord()).isTrue();
    assertThat(hasSourceIndex("Adults")).isTrue();
    assertEveryRowIsLinked("Adults");
    assertViewMatchesQuery();

    final long before = writes();
    database.transaction(() -> database.command("sql", "UPDATE Person SET age = 77 WHERE name = 'a100'"));
    assertThat(writes() - before).as("1 source update + 1 view row write, small slack").isLessThanOrEqualTo(4);
    assertThat(viewRows("Adults")).contains("a100/77");
    assertViewMatchesQuery();
  }

  @Test
  void legacyIncrementalViewIsUpgradedByReAlter() {
    makeLegacyIncrementalAdultsView(200);
    assertThat(view("Adults").isPerRecord()).isFalse();

    alter("Adults", "INCREMENTAL");

    assertThat(view("Adults").isPerRecord()).isTrue();
    assertThat(hasSourceIndex("Adults")).isTrue();
    assertEveryRowIsLinked("Adults");

    final long before = writes();
    database.transaction(() -> database.command("sql", "UPDATE Person SET age = 77 WHERE name = 'a100'"));
    assertThat(writes() - before).as("1 source update + 1 view row write, small slack").isLessThanOrEqualTo(4);
    assertViewMatchesQuery();

    reopenDatabase();
    assertThat(view("Adults").isPerRecord()).as("the upgrade is persisted").isTrue();
  }

  @Test
  void alterAwayFromIncrementalKeepsTheLink() {
    createPersons(20);
    createView("Adults", ADULTS_QUERY, MaterializedViewRefreshMode.INCREMENTAL);
    final Map<RID, RID> before = rowBySource("Adults");

    alter("Adults", "MANUAL");

    assertThat(view("Adults").isPerRecord()).isTrue();
    assertThat(hasSourceIndex("Adults")).isTrue();

    database.transaction(() -> database.command("sql", "UPDATE Person SET age = 60 WHERE name = 'a5'"));
    database.command("sql", "REFRESH MATERIALIZED VIEW Adults");

    final Map<RID, RID> after = rowBySource("Adults");
    assertThat(after).as("the full pass matches each row to its source, it does not reshuffle them").containsAllEntriesOf(before);
    assertThat(after).as("a5 entered the WHERE").hasSize(before.size() + 1);
    assertThat(viewRows("Adults")).contains("a5/60");
    assertViewMatchesQuery();
  }

  @Test
  void nonPerRecordQueryAlteredToIncrementalStaysFull() {
    createPersons(20);
    createView("Ordered", "SELECT name, age FROM Person ORDER BY name", MaterializedViewRefreshMode.MANUAL);

    alter("Ordered", "INCREMENTAL");

    assertThat(view("Ordered").isPerRecord()).isFalse();
    assertThat(database.getSchema().getType("Ordered").existsProperty(MaterializedViewPerRecordRefresher.SOURCE_RID)).isFalse();
  }

  @Test
  void sourceRenameKeepsPerRecordPassWorking() {
    createPersons(20);
    final MaterializedViewImpl view = createView("Adults", ADULTS_QUERY, MaterializedViewRefreshMode.INCREMENTAL);
    // the delta pass builds and caches its query, which names the source type
    database.transaction(() -> database.command("sql", "UPDATE Person SET age = 40 WHERE name = 'a1'"));

    database.command("sql", "ALTER TYPE Person NAME People");
    database.transaction(() -> database.newDocument("People").set("name", "newcomer").set("age", 30).save());

    assertThat(view.getStatus()).isEqualTo(MaterializedViewStatus.VALID.name());
    assertThat(viewRows("Adults")).contains("newcomer/30", "a1/40");
    assertThat(viewRows("Adults")).containsExactlyInAnyOrderElementsOf(queryRows("SELECT name, age FROM People WHERE age >= 18"));
  }

  @Test
  void viewOverAPerRecordViewStaysCorrect() {
    createPersons(30);
    createView("Adults", ADULTS_QUERY, MaterializedViewRefreshMode.INCREMENTAL);
    createView("AdultNames", "SELECT name, age FROM Adults", MaterializedViewRefreshMode.INCREMENTAL);
    assertThat(viewRows("AdultNames")).containsExactlyInAnyOrderElementsOf(viewRows("Adults"));

    database.transaction(() -> database.command("sql", "UPDATE Person SET age = 70 WHERE name = 'a20'"));
    database.transaction(() -> database.command("sql", "UPDATE Person SET age = 5 WHERE name = 'a21'"));
    database.transaction(() -> database.newDocument("Person").set("name", "newcomer").set("age", 33).save());
    database.transaction(() -> database.command("sql", "DELETE FROM Person WHERE name = 'a22'"));

    assertViewMatchesQuery();
    assertThat(viewRows("AdultNames")).contains("a20/70", "newcomer/33").doesNotContain("a21/5", "a22/32");
    assertThat(viewRows("AdultNames")).containsExactlyInAnyOrderElementsOf(queryRows("SELECT name, age FROM Adults"));

    database.command("sql", "REFRESH MATERIALIZED VIEW Adults");
    assertThat(viewRows("AdultNames")).containsExactlyInAnyOrderElementsOf(queryRows("SELECT name, age FROM Adults"));
    assertThat(view("AdultNames").getStatus()).isEqualTo(MaterializedViewStatus.VALID.name());
  }

  /**
   * A transaction captures changes against the listener of the view object an ALTER then replaces, and keeps writing after it:
   * the records changed before the ALTER (old listener) and after it (new listener) must both reach the view at commit.
   */
  @Test
  void transactionOpenAcrossAnAlterLosesNoChange() throws Exception {
    createPersons(20);
    createView("Adults", ADULTS_QUERY, MaterializedViewRefreshMode.INCREMENTAL);

    runAcrossAlter(() -> database.command("sql", "UPDATE Person SET age = 61 WHERE name = 'a1'"),
        () -> database.command("sql", "UPDATE Person SET age = 62 WHERE name = 'a2'"));

    assertThat(viewRows("Adults")).contains("a1/61", "a2/62");
    assertViewMatchesQuery();
    assertThat(view("Adults").getStatus()).isEqualTo(MaterializedViewStatus.VALID.name());
  }

  /**
   * The upgrade replaces a legacy view whose listener refreshes the whole view: a transaction that registered that refresh before
   * the ALTER and commits after it must not rewrite the upgraded view the legacy way, which drops the source link from every row.
   */
  @Test
  void transactionOpenAcrossTheUpgradeKeepsTheLink() throws Exception {
    makeLegacyIncrementalAdultsView(20);

    runAcrossAlter(() -> database.command("sql", "UPDATE Person SET age = 61 WHERE name = 'a1'"),
        () -> database.command("sql", "UPDATE Person SET age = 62 WHERE name = 'a2'"));

    assertThat(view("Adults").isPerRecord()).isTrue();
    assertEveryRowIsLinked("Adults");
    assertThat(viewRows("Adults")).contains("a1/61", "a2/62");
    assertViewMatchesQuery();

    database.transaction(() -> database.command("sql", "UPDATE Person SET age = 63 WHERE name = 'a3'"));
    assertThat(viewRows("Adults")).contains("a3/63");
    assertViewMatchesQuery();
  }

  /**
   * A failed pass leaves the view owing a full pass, and that debt is state of the view object an ALTER replaces: the replacement
   * must still owe it, or the next delta pass ends VALID over a view missing what the failed pass dropped.
   */
  @Test
  void aFullPassOwedBeforeAnAlterIsStillOwedAfterIt() {
    createPersons(20);
    createView("Adults", ADULTS_QUERY, MaterializedViewRefreshMode.INCREMENTAL);
    failNextPassOf("Adults", () -> database.transaction(() -> database.command("sql", "UPDATE Person SET age = 50 WHERE name = 'a1'")));
    assertThat(view("Adults").getStatus()).isNotEqualTo(MaterializedViewStatus.VALID.name());

    alter("Adults", "INCREMENTAL");
    database.transaction(() -> database.command("sql", "UPDATE Person SET age = 51 WHERE name = 'a2'"));

    Awaitility.await("the view ends VALID").atMost(30, TimeUnit.SECONDS)
        .until(() -> MaterializedViewStatus.VALID.name().equals(view("Adults").getStatus()));
    assertThat(viewRows("Adults")).contains("a1/50", "a2/51");
    assertViewMatchesQuery();
  }

  /**
   * The upgrade saves the schema with the flag set before its full pass writes the link on any row. A crash in between must reopen
   * as a view that owes a full pass, not as a VALID per-record view over unlinked rows, whose next delta pass finds no row by the
   * link and inserts a duplicate. Simulated by capturing schema.json while the upgrade pass runs, failing that pass (so no row
   * gets the link), and reopening on the captured file.
   */
  @Test
  void crashDuringTheUpgradePassReopensAsAViewOwingAFullPass() throws Exception {
    makeLegacyIncrementalAdultsView(20);
    final Path schemaFile = Path.of(database.getDatabasePath(), LocalSchema.SCHEMA_FILE_NAME);
    final AtomicReference<byte[]> onDiskDuringThePass = new AtomicReference<>();
    final DocumentType backing = database.getSchema().getType("Adults");
    final BeforeRecordCreateListener crash = record -> {
      try {
        onDiskDuringThePass.compareAndSet(null, Files.readAllBytes(schemaFile));
      } catch (final Exception e) {
        throw new IllegalStateException(e);
      }
      throw new IllegalStateException("simulated crash during the upgrade pass");
    };
    backing.getEvents().registerListener(crash);
    try {
      assertThatThrownBy(() -> alter("Adults", "INCREMENTAL")).hasStackTraceContaining("simulated crash during the upgrade pass");
    } finally {
      backing.getEvents().unregisterListener(crash);
    }
    assertThat(onDiskDuringThePass.get()).as("the upgrade pass reached the backing type").isNotNull();

    database.close();
    Files.write(schemaFile, onDiskDuringThePass.get());
    database = factory.open();

    final MaterializedViewImpl reopened = view("Adults");
    assertThat(reopened.isPerRecord()).isTrue();
    assertThat(reopened.getStatus()).as("an interrupted upgrade is not VALID").isNotEqualTo(MaterializedViewStatus.VALID.name());

    database.transaction(() -> database.command("sql", "UPDATE Person SET age = 64 WHERE name = 'a4'"));
    Awaitility.await("the recovery full pass ends VALID").atMost(30, TimeUnit.SECONDS)
        .until(() -> MaterializedViewStatus.VALID.name().equals(view("Adults").getStatus()));
    assertEveryRowIsLinked("Adults");
    assertThat(viewRows("Adults")).contains("a4/64");
    assertViewMatchesQuery();
  }

  /**
   * A failed upgrade pass must not fail the schema frame of the ALTER: under HA a frame that throws ships nothing, so followers
   * would never get the link, its index or the flag the leader already has, and the next pass on the leader would write pages of
   * an index file they lack. The frame returns, and the ALTER reports after it that the mode was changed and the view is in ERROR
   * with a full pass owed, which the next commit gets made.
   */
  @Test
  void failedUpgradePassReportsTheSwitchedViewAndRecoversOnTheNextCommit() {
    makeLegacyIncrementalAdultsView(20);
    final DocumentType backing = database.getSchema().getType("Adults");
    final BeforeRecordCreateListener boom = record -> {
      throw new IllegalStateException("simulated failure of the upgrade pass");
    };
    final SchemaSessionCounter.Count seen = new SchemaSessionCounter.Count();
    backing.getEvents().registerListener(boom);
    try {
      assertThatThrownBy(() -> SchemaSessionCounter.count((LocalDatabase) database, seen, () -> alter("Adults", "INCREMENTAL")))
          .hasMessageContaining("Materialized view 'Adults' was switched to REFRESH INCREMENTAL")
          .hasMessageContaining("ERROR")
          .hasMessageContaining("REFRESH MATERIALIZED VIEW Adults")
          .hasStackTraceContaining("simulated failure of the upgrade pass");
    } finally {
      backing.getEvents().unregisterListener(boom);
    }
    assertThat(seen.sessions()).as("the ALTER opened its schema frame").isPositive();
    assertThat(seen.failed()).as("the schema frame of the ALTER returned normally, so HA ships the link, its index and the flag")
        .isZero();
    assertThat(view("Adults").isPerRecord()).isTrue();
    assertThat(hasSourceIndex("Adults")).isTrue();
    assertThat(view("Adults").getStatus()).isEqualTo(MaterializedViewStatus.ERROR.name());

    database.transaction(() -> database.command("sql", "UPDATE Person SET age = 65 WHERE name = 'a5'"));
    Awaitility.await("the recovery full pass ends VALID").atMost(30, TimeUnit.SECONDS)
        .until(() -> MaterializedViewStatus.VALID.name().equals(view("Adults").getStatus()));
    assertEveryRowIsLinked("Adults");
    assertThat(viewRows("Adults")).contains("a5/65");
    assertViewMatchesQuery();
  }

  /**
   * The straddle window of the upgrade: a legacy whole-view pass reads its guard while the view is still its own, the whole ALTER
   * runs (link, index, upgrade pass, registration), and only then does the legacy pass read the view and write its rows, without
   * the link and with no conflict, since it read nothing before the ALTER committed. Its owner must find the replacement after
   * the pass and pass a full pass on to it, which deletes the unlinked rows; ending there left the upgraded view VALID over rows
   * no delta pass can find.
   */
  @Test
  void aLegacyPassOvertakenByTheWholeUpgradeIsRepairedByItsOwner() {
    makeLegacyIncrementalAdultsView(20);
    final LocalSchema schema = (LocalSchema) database.getSchema();
    final MaterializedViewImpl legacy = view("Adults");
    final AtomicReference<Throwable> alterFailure = new AtomicReference<>();
    final AtomicBoolean armed = new AtomicBoolean(true);
    final MaterializedViewImpl straddling = new MaterializedViewImpl(database, legacy.getName(), legacy.getQuery(),
        legacy.getBackingTypeName(), legacy.getSourceTypeNames(), legacy.getRefreshMode(), legacy.isSimpleQuery(),
        legacy.getRefreshInterval()) {
      @Override
      MaterializedViewImpl current() {
        // The guard of the legacy pass, the first read made inside its transaction: it finds the view still its own, and the
        // whole ALTER runs before the pass reads anything else
        if (getDatabase().isTransactionActive() && armed.compareAndSet(true, false)) {
          final Thread alterThread = new Thread(() -> {
            try {
              alter("Adults", "INCREMENTAL");
            } catch (final Throwable e) {
              alterFailure.set(e);
            }
          }, "mv-straddling-alter");
          alterThread.start();
          try {
            alterThread.join(60_000);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
          }
          return this;
        }
        return super.current();
      }
    };
    // The registered legacy instance is swapped for the straddling one, with no listener: the commit below reaches the view
    // through the upgrade pass only, and the legacy pass is driven directly, as its owner makes it
    MaterializedViewBuilder.unregisterListeners(schema, legacy);
    synchronized (schema) {
      schema.materializedViews.put("Adults", straddling);
    }
    database.transaction(() -> database.command("sql", "UPDATE Person SET age = 68 WHERE name = 'a8'"));

    MaterializedViewRefresher.refreshAfterCommit(database, straddling);

    assertThat(armed.get()).as("the legacy pass read its guard inside its transaction").isFalse();
    assertThat(alterFailure.get()).isNull();
    assertThat(view("Adults")).isNotSameAs(straddling);
    assertThat(view("Adults").isPerRecord()).isTrue();
    assertEveryRowIsLinked("Adults");
    assertThat(viewRows("Adults")).contains("a8/68");
    assertViewMatchesQuery();
    assertThat(view("Adults").getStatus()).isEqualTo(MaterializedViewStatus.VALID.name());
  }

  /**
   * A pass the replaced instance passes on fails on the registered view: that failure is the registered view's alone. Handled
   * again by the replaced instance's own failure path, it counted an error against an instance no longer registered and released
   * an ownership already given back, which a later owner of that instance may hold.
   */
  @Test
  void aFailedPassPassedOnIsHandledOnceByTheRegisteredView() {
    makeLegacyIncrementalAdultsView(20);
    final MaterializedViewImpl legacy = view("Adults");
    alter("Adults", "INCREMENTAL");
    final MaterializedViewImpl upgraded = view("Adults");
    final long legacyErrors = legacy.getErrorCount();
    final long upgradedErrors = upgraded.getErrorCount();

    failNextPassOf("Adults", () -> assertThatThrownBy(() -> MaterializedViewRefresher.refreshAfterCommit(database, legacy))
        .hasStackTraceContaining("simulated failure part way through the refresh"));

    assertThat(upgraded.getErrorCount()).as("the registered view made the pass and failed it").isEqualTo(upgradedErrors + 1);
    assertThat(legacy.getErrorCount()).as("the replaced instance handed the pass on and did not fail it too").isEqualTo(legacyErrors);
    assertThat(legacy.getStatus()).isEqualTo(MaterializedViewStatus.VALID.name());
    assertThat(legacy.tryBeginRefresh()).as("the replaced instance was released once, by the hand-over").isTrue();
    legacy.endRefresh();
  }

  /**
   * A refresh the OLD instance owns when the upgrade ALTER replaces it (a pass still running, a further pass owed, or one handed
   * to the scheduler) makes its next pass on that instance. For a legacy view that pass was a whole-view rewrite, which drops the
   * source link from every row of the upgraded view; it must be passed on to the registered view instead.
   */
  @Test
  void aPassOfTheReplacedInstanceIsPassedToTheRegisteredView() {
    makeLegacyIncrementalAdultsView(20);
    final MaterializedViewImpl legacy = view("Adults");

    alter("Adults", "INCREMENTAL");
    assertEveryRowIsLinked("Adults");

    // A commit whose request reached only the replaced instance (its callback resolved the view before the swap): the registered
    // view's listener is kept off for it, so only the request passed on from the replaced instance can bring a6 into the view
    final LocalSchema schema = (LocalSchema) database.getSchema();
    final MaterializedViewImpl upgraded = view("Adults");
    MaterializedViewBuilder.unregisterListeners(schema, upgraded);
    database.transaction(() -> database.command("sql", "UPDATE Person SET age = 66 WHERE name = 'a6'"));
    MaterializedViewBuilder.registerListeners(schema, upgraded, upgraded.getSourceTypeNames());
    assertThat(viewRows("Adults")).doesNotContain("a6/66");

    // the next pass of the replaced instance, as its running owner, a coalesced request or the scheduler makes it
    MaterializedViewRefresher.refreshAfterCommit(database, legacy);

    assertThat(view("Adults").isPerRecord()).isTrue();
    assertEveryRowIsLinked("Adults");
    assertThat(viewRows("Adults")).contains("a6/66");
    assertViewMatchesQuery();
    assertThat(view("Adults").getStatus()).isEqualTo(MaterializedViewStatus.VALID.name());

    database.transaction(() -> database.command("sql", "UPDATE Person SET age = 67 WHERE name = 'a7'"));
    assertViewMatchesQuery();
  }

  // ---- helpers ----

  /**
   * Runs {@code beforeAlter} and {@code afterAlter} in ONE transaction on another thread, with an ALTER of the view to INCREMENTAL
   * made on this thread in between, then commits it.
   */
  private void runAcrossAlter(final Runnable beforeAlter, final Runnable afterAlter) throws Exception {
    final CountDownLatch captured = new CountDownLatch(1);
    final CountDownLatch altered = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread writer = new Thread(() -> {
      try {
        database.begin();
        beforeAlter.run();
        captured.countDown();
        assertThat(altered.await(30, TimeUnit.SECONDS)).isTrue();
        afterAlter.run();
        database.commit();
      } catch (final Throwable e) {
        failure.set(e);
        captured.countDown();
        if (database.isTransactionActive())
          database.rollback();
      }
    }, "mv-alter-writer");
    writer.start();
    assertThat(captured.await(30, TimeUnit.SECONDS)).isTrue();
    try {
      alter("Adults", "INCREMENTAL");
    } finally {
      altered.countDown();
    }
    writer.join(60_000);
    assertThat(writer.isAlive()).isFalse();
    assertThat(failure.get()).isNull();
  }

  /**
   * An INCREMENTAL view as one created before #11475 looks: no flag, no index, no property, rows without the link. Built as a
   * per-record view, stripped of its link in the schema, reopened, and refreshed once by a commit, whose legacy full rewrite
   * leaves rows without the link.
   */
  private void makeLegacyIncrementalAdultsView(final int count) {
    createPersons(count);
    final MaterializedViewImpl view = createView("Adults", ADULTS_QUERY, MaterializedViewRefreshMode.INCREMENTAL);
    view.setPerRecord(false);
    final DocumentType backing = database.getSchema().getType("Adults");
    for (final Index index : backing.getAllIndexes(false))
      if (index.getPropertyNames().equals(List.of(MaterializedViewPerRecordRefresher.SOURCE_RID)))
        database.getSchema().dropIndex(index.getName());
    backing.dropProperty(MaterializedViewPerRecordRefresher.SOURCE_RID);
    ((LocalSchema) database.getSchema()).saveConfiguration();
    reopenDatabase();

    assertThat(view("Adults").isPerRecord()).isFalse();
    database.transaction(() -> database.command("sql", "UPDATE Person SET age = 30 WHERE name = 'a0'"));
    try (final ResultSet rs = database.query("sql", "SELECT FROM Adults")) {
      while (rs.hasNext())
        assertThat(rs.next().<Object>getProperty(MaterializedViewPerRecordRefresher.SOURCE_RID)).isNull();
    }
    assertThat(hasSourceIndex("Adults")).isFalse();
    assertViewMatchesQuery();
  }

  private void alter(final String viewName, final String mode) {
    database.command("sql", "ALTER MATERIALIZED VIEW " + viewName + " REFRESH " + mode);
  }

  private MaterializedViewImpl view(final String name) {
    return (MaterializedViewImpl) database.getSchema().getMaterializedView(name);
  }

  private boolean hasSourceIndex(final String viewName) {
    for (final Index index : database.getSchema().getType(viewName).getAllIndexes(false))
      if (index.isUnique() && index.getPropertyNames().equals(List.of(MaterializedViewPerRecordRefresher.SOURCE_RID)))
        return true;
    return false;
  }

  private void assertEveryRowIsLinked(final String viewName) {
    final Map<RID, RID> rows = rowBySource(viewName);
    assertThat(rows.keySet()).doesNotContainNull();
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM (" + ADULTS_QUERY + ")")) {
      assertThat(rows).hasSize(rs.next().<Number>getProperty("c").intValue());
    }
  }

  private void failNextPassOf(final String viewName, final Runnable commit) {
    final DocumentType backing = database.getSchema().getType(viewName);
    final AtomicInteger written = new AtomicInteger();
    final BeforeRecordCreateListener boomOnCreate = record -> {
      if (written.incrementAndGet() == 1)
        throw new IllegalStateException("simulated failure part way through the refresh");
      return true;
    };
    final BeforeRecordUpdateListener boomOnUpdate = record -> {
      if (written.incrementAndGet() == 1)
        throw new IllegalStateException("simulated failure part way through the refresh");
      return true;
    };
    backing.getEvents().registerListener(boomOnCreate);
    backing.getEvents().registerListener(boomOnUpdate);
    try {
      commit.run();
    } finally {
      backing.getEvents().unregisterListener(boomOnCreate);
      backing.getEvents().unregisterListener(boomOnUpdate);
    }
    assertThat(written.get()).as("the pass reached the backing type and failed there").isPositive();
  }

  private void assertViewMatchesQuery() {
    assertThat(viewRows("Adults")).containsExactlyInAnyOrderElementsOf(queryRows(ADULTS_QUERY));
  }

  private long writes() {
    final Map<String, Object> stats = database.getStats();
    return ((Number) stats.get("updateRecord")).longValue() + ((Number) stats.get("createRecord")).longValue()
        + ((Number) stats.get("deleteRecord")).longValue();
  }

  /** Ages 10..49 cycling, so about three quarters of the records match {@link #ADULTS_QUERY}. */
  private void createPersons(final int count) {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Person");
      type.createProperty("name", Type.STRING);
      type.createProperty("age", Type.INTEGER);
    });
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        database.newDocument("Person").set("name", "a" + i).set("age", 10 + (i % 40)).save();
    });
  }

  private MaterializedViewImpl createView(final String name, final String query, final MaterializedViewRefreshMode mode) {
    database.transaction(() -> database.getSchema().buildMaterializedView()
        .withName(name)
        .withQuery(query)
        .withRefreshMode(mode)
        .create());
    return view(name);
  }

  private Map<RID, RID> rowBySource(final String viewName) {
    final Map<RID, RID> map = new HashMap<>();
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + viewName)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final RID source = row.getProperty(MaterializedViewPerRecordRefresher.SOURCE_RID);
        assertThat(map.put(source, row.getIdentity().get())).as("one row per source record").isNull();
      }
    }
    return map;
  }

  private List<String> viewRows(final String viewName) {
    return queryRows("SELECT name, age FROM " + viewName);
  }

  private List<String> queryRows(final String sql) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        rows.add(row.getProperty("name") + "/" + row.getProperty("age"));
      }
    }
    return rows;
  }
}
