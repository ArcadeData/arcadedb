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

package com.arcadedb.database;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.WALFile;
import com.arcadedb.exception.TransactionCommittedRemotelyException;
import com.arcadedb.exception.TransactionOutcomeUnknownException;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests of the database engine: DatabaseConfiguration. Each nested class keeps the regression of one reported issue (see the
 * Javadoc of each).
 */
public class DatabaseConfigurationTest {
  /**
   * Issue #10108: under Raft HA the entry a configuration change replicates named the setting its statement changed (issue
   * #9959) but carried the value the in-memory configuration held WHEN IT WAS SUBMITTED, which is not necessarily the value that
   * statement set: a second statement on the same setting can store its own value in between. Statement A then shipped B's value,
   * and when B's own save was refused, B's caller was told nothing had changed anywhere in the cluster while A's entry had
   * committed B's value cluster-wide.
   * <p>
   * The change now carries the entry, captured under the lock the value is stored under, so what is submitted is what that
   * statement set whatever the others do. The tests pin the CONTENT of each submitted entry: the submit is modelled by recording
   * the entry and leaving the file alone, since under HA the file is written by the apply of the entry, after the submit.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SettingValueSubmitted {
    private static final String DB_PATH = "target/databases/SettingValueSubmitted";
    private static final String KEY     = GlobalConfiguration.TYPE_DEFAULT_BUCKETS.getKey();

    /** Records what the Raft HA wrapper would submit for a change, at its save. */
    private static final class RecordingDatabase extends LocalDatabase {
      private final List<String> submitted = new ArrayList<>();

      private RecordingDatabase(final String path) {
        super(path, ComponentFile.MODE.READ_WRITE, new ContextConfiguration(), null, new ConcurrentHashMap<>());
      }

      @Override
      public void saveConfiguration(final SettingChange change) throws IOException {
        submitted.add(pendingConfigurationChange(change));
      }
    }

    private RecordingDatabase database;

    @BeforeEach
    void setUp() {
      FileUtils.deleteRecursively(new File(DB_PATH));
      PageManager.INSTANCE.acquire();
      database = new RecordingDatabase(DB_PATH);
      database.create();
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    private LocalDatabase.SettingChange set(final Object value) {
      return database.changeSetting(KEY, () -> database.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, value));
    }

    @Test
    void twoStatementsOnTheSameSettingEachSubmitTheirOwnValue() throws IOException {
      final LocalDatabase.SettingChange a = set(5);
      final LocalDatabase.SettingChange b = set(7);
      assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).as("memory holds B's value")
          .isEqualTo(7);

      // A reaches its save after B has stored: its entry still carries what A set
      database.saveConfiguration(a);
      database.saveConfiguration(b);

      assertThat(database.submitted).hasSize(2);
      assertThat(new JSONObject(database.submitted.get(0)).getJSONObject("configuration").getInt("typeDefaultBuckets"))
          .as("A's entry carries A's value, not the one B stored meanwhile").isEqualTo(5);
      assertThat(new JSONObject(database.submitted.get(1)).getJSONObject("configuration").getInt("typeDefaultBuckets"))
          .as("B's entry carries B's value").isEqualTo(7);
    }

    @Test
    void theEntryNamesTheSettingAloneWhateverElseIsPending() throws IOException {
      final LocalDatabase.SettingChange a = set(5);
      database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 2);

      database.saveConfiguration(a);

      final JSONObject entry = new JSONObject(database.submitted.getFirst());
      assertThat(entry.getJSONObject("configuration").keySet()).containsExactly("typeDefaultBuckets");
      assertThat(entry.getJSONArray("removed").length()).isZero();
    }

    @Test
    void aSettingTheStatementRemovedIsSubmittedAsRemovedWhateverStoredItAfterwards() throws IOException {
      database.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 5);
      database.saveConfiguration();

      final LocalDatabase.SettingChange removal = set(null);
      set(9);

      database.saveConfiguration(removal);

      final JSONObject entry = new JSONObject(database.submitted.getFirst());
      assertThat(entry.getJSONObject("configuration").keySet()).isEmpty();
      assertThat(entry.getJSONArray("removed").toList()).containsExactly("typeDefaultBuckets");
    }

    @Test
    void aValueTheFileAlreadyHoldsSubmitsNothing() throws IOException {
      final LocalDatabase.SettingChange a = set(5);
      database.saveConfiguration();

      database.saveConfiguration(a);

      assertThat(database.submitted.getFirst()).as("the value set was already the stored one").isNull();
    }

    /** The value is compared with what the file holds by its text, so a type that renders differently once stored would resubmit. */
    @Test
    void aBooleanAnIntegerAndAStringAreEachTheSameAsTheFileOnceSaved() throws IOException {
      final LocalDatabase.SettingChange flag = database.changeSetting(GlobalConfiguration.TX_WAL.getKey(),
          () -> database.getConfiguration().setValue(GlobalConfiguration.TX_WAL, false));
      final LocalDatabase.SettingChange number = database.changeSetting(GlobalConfiguration.TX_WAL_FLUSH.getKey(),
          () -> database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 2));
      final LocalDatabase.SettingChange text = database.changeSetting("custom", () -> database.getConfiguration().setValue("custom", "x"));

      database.saveConfiguration(flag);
      database.saveConfiguration(number);
      database.saveConfiguration(text);

      assertThat(new JSONObject(database.submitted.get(0)).getJSONObject("configuration")
          .getBoolean(ContextConfiguration.jsonKey(GlobalConfiguration.TX_WAL.getKey()))).isFalse();
      assertThat(new JSONObject(database.submitted.get(1)).getJSONObject("configuration").getInt("txWalFlush")).isEqualTo(2);
      assertThat(new JSONObject(database.submitted.get(2)).getJSONObject("configuration").getString("custom")).isEqualTo("x");

      // Written to the file the way an apply does, then submitted again: nothing is left to ship for any of them
      database.saveConfiguration();
      database.saveConfiguration(flag);
      database.saveConfiguration(number);
      database.saveConfiguration(text);
      assertThat(database.submitted.subList(3, 6)).containsOnlyNulls();
    }

    @Test
    void aChangeMadeWithoutTheApplyLockFallsBackToTheValueMemoryHolds() throws IOException {
      database.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 5);

      database.saveConfiguration(new LocalDatabase.SettingChange(KEY, null, 5));

      assertThat(new JSONObject(database.submitted.getFirst()).getJSONObject("configuration").getInt("typeDefaultBuckets"))
          .isEqualTo(5);
    }

    /** The writer: ALTER DATABASE submits through the change it made, so its entry is the value it set. */
    @Test
    void alterDatabaseSubmitsTheValueItSet() {
      database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 4");

      assertThat(database.submitted).hasSize(1);
      assertThat(new JSONObject(database.submitted.getFirst()).getJSONObject("configuration").getInt("typeDefaultBuckets"))
          .isEqualTo(4);
    }
  }

  /**
   * Issue #10155: a database setting changed on an open database re-derived two of its four query caches. The SQL and Cypher PLAN
   * caches were never cleared, and the plan cache is asked before anything is re-parsed, so a setting read at planning time -
   * {@code arcadedb.queryIndexMaxSelectivity}, baked into the plan as a scan-fallback step - kept the plan it was baked into until a
   * DDL statement or a reopen. And the three settings that SIZE the caches were read once, by the constructor.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SettingChangeClearsQueryCaches extends TestHelper {
    private static final String SQL    = "select from S";
    private static final String CYPHER = "MATCH (n:S) RETURN n";

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType("S").createProperty("v", Type.INTEGER);
        database.getSchema().getType("S").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "v");
        for (int i = 0; i < 60; i++)
          database.newVertex("S").set("v", i % 2).save();
      });
    }

    /**
     * The invariant, over every cache a setting change has to empty: a fifth cache added later is either on this list or visibly
     * absent from it.
     */
    @Test
    void aSettingChangeEmptiesEveryQueryCache() {
      final LocalDatabase db = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
      final Map<String, BooleanSupplier> caches = new LinkedHashMap<>();
      caches.put("SQL statement cache", () -> db.getStatementCache().contains(SQL));
      caches.put("SQL execution plan cache", () -> db.getExecutionPlanCache().contains(SQL));
      caches.put("Cypher statement cache", () -> db.getCypherStatementCache().size() > 0);
      caches.put("Cypher plan cache", () -> db.getCypherPlanCache().size() > 0);

      for (int i = 0; i < 2; i++) {
        database.query("sql", SQL).close();
        database.query("opencypher", CYPHER).close();
      }
      caches.forEach((name, holds) -> assertThat(holds.getAsBoolean()).as("%s holds the query before the change", name).isTrue());

      database.command("sql", "ALTER DATABASE `arcadedb.queryIndexMaxSelectivity` 0.5").close();

      caches.forEach((name, holds) -> assertThat(holds.getAsBoolean()).as("%s after a setting change", name).isFalse());
    }

    /** The repro of the issue: a cached plan carried the scan fallback of a selectivity the database no longer runs with. */
    @Test
    void aSettingReadAtPlanningTimeReachesTheNextExecutionOfACachedQuery() {
      final String query = "select count(*) as c from S where v = 1";

      database.command("sql", "ALTER DATABASE `arcadedb.queryIndexMaxSelectivity` 0.6").close();
      // twice: the second execution is served by the cached plan
      assertThat(planOf(query)).contains("full scan of S");
      assertThat(planOf(query)).contains("full scan of S");

      database.command("sql", "ALTER DATABASE `arcadedb.queryIndexMaxSelectivity` 0").close();
      assertThat(planOf(query)).as("the plan is rebuilt with the selectivity now in force").doesNotContain("full scan of S");
    }

    @Test
    void theCacheSizesFollowTheirSettingsOnAnOpenDatabase() {
      final LocalDatabase db = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();

      database.command("sql", "ALTER DATABASE `arcadedb.opencypher.planCache` 1").close();
      database.command("sql", "ALTER DATABASE `arcadedb.opencypher.statementCache` 1").close();
      database.command("sql", "ALTER DATABASE `arcadedb.sqlStatementCache` 1").close();

      for (final String label : new String[] { "a", "b", "c" }) {
        database.query("opencypher", "MATCH (n:S) RETURN n AS " + label).close();
        database.query("sql", "select v as " + label + " from S").close();
      }

      assertThat(db.getCypherPlanCache().size()).isEqualTo(1);
      assertThat(db.getCypherStatementCache().size()).isEqualTo(1);
      assertThat(db.getExecutionPlanCache().contains("select v as c from S")).isTrue();
      assertThat(db.getExecutionPlanCache().contains("select v as a from S")).as("evicted by a cache of one").isFalse();
      assertThat(db.getStatementCache().contains("select v as a from S")).as("evicted by a cache of one").isFalse();

      // a size of 0 caches nothing, and every query still answers
      database.command("sql", "ALTER DATABASE `arcadedb.sqlStatementCache` 0").close();
      database.query("sql", "select v as zero from S").close();
      assertThat(db.getExecutionPlanCache().contains("select v as zero from S")).isFalse();
      try (final ResultSet rs = database.query("sql", "select count(*) as c from S")) {
        assertThat(rs.next().<Long>getProperty("c")).isEqualTo(60L);
      }
    }

    /**
     * The caches are resized under the same monitors their lookups take, so queries running while settings keep changing see a
     * cache either before or after a resize, never half of one: every query answers, and nothing escapes from inside a cache.
     */
    @Test
    void queriesRacingSettingChangesAlwaysAnswer() throws Exception {
      final int threads = 4;
      final AtomicBoolean stop = new AtomicBoolean();
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final ExecutorService pool = Executors.newFixedThreadPool(threads);
      try {
        for (int t = 0; t < threads; t++) {
          final int thread = t;
          pool.submit(() -> {
            int i = 0;
            while (!stop.get() && failure.get() == null)
              try {
                // distinct texts, so the lookups insert and evict while the caches are being resized
                final String label = "c" + thread + "_" + (i++ % 50);
                try (final ResultSet rs = database.query("sql", "select count(*) as " + label + " from S where v = 1")) {
                  assertThat(rs.next().<Long>getProperty(label)).isEqualTo(30L);
                }
                try (final ResultSet rs = database.query("opencypher", "MATCH (n:S) WHERE n.v = 1 RETURN count(n) AS " + label)) {
                  assertThat(rs.next().<Long>getProperty(label)).isEqualTo(30L);
                }
              } catch (final Throwable e) {
                failure.compareAndSet(null, e);
              }
          });
        }

        for (int i = 0; i < 200 && failure.get() == null; i++) {
          final int size = 1 + i % 4;
          database.command("sql", "ALTER DATABASE `arcadedb.sqlStatementCache` " + size).close();
          database.command("sql", "ALTER DATABASE `arcadedb.opencypher.planCache` " + size).close();
          database.command("sql", "ALTER DATABASE `arcadedb.opencypher.statementCache` " + size).close();
        }
      } finally {
        stop.set(true);
        pool.shutdown();
        assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).as("query threads ended").isTrue();
      }
      assertThat(failure.get()).as("a query racing the resizes").isNull();
    }

    private String planOf(final String query) {
      try (final ResultSet resultSet = database.query("sql", query)) {
        assertThat(resultSet.next().<Long>getProperty("c")).isEqualTo(30L);
        return resultSet.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      }
    }
  }

  /**
   * Issue #6114: {@code configuration.json} is written atomically, so a concurrent reader can never observe it
   * truncated.
   * <p>
   * {@code LocalDatabase.saveConfiguration()} used a plain {@code FileOutputStream}, which truncates the file to zero
   * and only then refills it. Anything reading it by name in that window - the full backup's lock-free t0 capture
   * above all - could see an empty or half-written document, and a crash there left the database without its
   * settings file.
   * <p>
   * The schema half of the same issue ({@code schema.json} / {@code schema.prev.json}) is covered by
   * {@code com.arcadedb.schema.SchemaFileDurabilityTest.AtomicSchemaWriteTest}; this class deliberately does not repeat it.
   * <p>
   * Killing the JVM mid-write is not something a unit test can do, so the equivalent observation is made from another
   * thread: a reader polling the file as fast as it can, while it is rewritten over and over, must never see it
   * missing, empty, or unparseable. Against the previous code this fails within a handful of iterations.
   */
  @Nested
  class AtomicConfigurationWrite extends TestHelper {

    /** Enough saves that a poller running flat out lands inside a write window many times over. */
    private static final int SAVES = 300;

    @Test
    void configurationJsonIsNeverObservablyAbsentOrPartial() throws Exception {
      final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
      final File configurationFile = local.getConfigurationFile();

      final List<String> defects = pollWhile(configurationFile, () -> {
        for (int i = 0; i < SAVES; i++)
          local.saveConfiguration();
      });

      assertThat(defects).as("configuration.json must never be observed absent or half-written").isEmpty();
      assertThat(configurationFile).exists();
      assertThat(configurationFile.getParentFile().list((dir, name) -> name.startsWith(LocalDatabase.CONFIGURATION_FILE_NAME)
          && name.endsWith(".tmp"))).as("the atomic write must not leave its scratch file behind").isEmpty();
    }

    /**
     * Runs {@code writer} on this thread while another thread reads {@code file} in a tight loop, and returns one
     * description per defective observation: the file missing, empty, or not a parseable JSON document.
     */
    private List<String> pollWhile(final File file, final ThrowingRunnable writer) throws Exception {
      final List<String> defects = new ArrayList<>();
      final AtomicBoolean polling = new AtomicBoolean(true);
      final AtomicLong reads = new AtomicLong();
      final AtomicReference<Exception> pollerFailure = new AtomicReference<>();
      final CountDownLatch started = new CountDownLatch(1);

      final Thread poller = new Thread(() -> {
        started.countDown();
        while (polling.get()) {
          try {
            if (!file.exists()) {
              synchronized (defects) {
                defects.add("absent");
              }
              continue;
            }
            final String content = Files.readString(file.toPath(), StandardCharsets.UTF_8);
            reads.incrementAndGet();
            if (content.isEmpty())
              synchronized (defects) {
                defects.add("empty");
              }
            else
              try {
                new JSONObject(content);
              } catch (final RuntimeException e) {
                synchronized (defects) {
                  defects.add("unparseable (" + content.length() + " chars): " + e.getMessage());
                }
              }
          } catch (final java.nio.file.NoSuchFileException e) {
            synchronized (defects) {
              defects.add("vanished between exists() and read");
            }
          } catch (final Exception e) {
            pollerFailure.compareAndSet(null, e);
            return;
          }
        }
      }, "issue6114-config-poller");
      poller.setDaemon(true);
      poller.start();

      try {
        assertThat(started.await(30, TimeUnit.SECONDS)).isTrue();
        writer.run();
      } finally {
        polling.set(false);
        poller.join(30_000);
      }

      assertThat(pollerFailure.get()).isNull();
      assertThat(reads.get()).as("the poller must actually have read the file while it was being rewritten")
          .isGreaterThan(10);

      synchronized (defects) {
        return new ArrayList<>(defects);
      }
    }

    private interface ThrowingRunnable {
      void run() throws Exception;
    }
  }

  /**
   * Issue #10790: the global-variable store holds a key as given, for a key space that is not SQL's (the Redis wrapper), while the
   * SQL accessors keep their rules - one leading {@code $} stripped, the reserved names refused - and a name SQL reserves stays the
   * context's: an entry the store holds under it never answers a SQL {@code $parent}, {@code $depth}...
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GlobalVariableStore extends TestHelper {

    @Test
    void theStoreHoldsEveryKeyAsGiven() {
      final ConcurrentMap<String, Object> store = ((DatabaseInternal) database).getGlobalVariableStore();
      try {
        for (final String name : SQLQueryEngine.RESERVED_VARIABLE_NAMES) {
          store.put(name, "plain-" + name);
          store.put("$" + name, "dollar-" + name);
          assertThat(store.get(name)).isEqualTo("plain-" + name);
          assertThat(store.get("$" + name)).isEqualTo("dollar-" + name);
        }
        store.put("k10790", "plain");
        store.put("$k10790", "dollar");
        assertThat(store.get("k10790")).as("$k and k are two keys of the store").isEqualTo("plain");
        assertThat(store.get("$k10790")).isEqualTo("dollar");

        // the SQL accessors keep their rules
        assertThat(((DatabaseInternal) database).getGlobalVariable("$k10790")).as("SQL strips one $").isEqualTo("plain");
        assertThatThrownBy(() -> ((DatabaseInternal) database).setGlobalVariable("parent", "x")).hasMessageContaining("reserved");
      } finally {
        store.clear();
      }
    }

    @Test
    void aReservedNameInTheStoreNeverAnswersForTheSqlContextVariable() {
      final ConcurrentMap<String, Object> store = ((DatabaseInternal) database).getGlobalVariableStore();
      try {
        for (final String name : SQLQueryEngine.RESERVED_VARIABLE_NAMES)
          store.put(name, "leaked-" + name);
        store.put("visible10790", "shared");

        for (final String name : SQLQueryEngine.RESERVED_VARIABLE_NAMES)
          assertThat(((DatabaseInternal) database).getGlobalVariable(name)).as("$" + name + " is the context's").isNull();

        try (final ResultSet rs = database.query("sql", "SELECT $parent AS parent, $depth AS depth, $visible10790 AS visible")) {
          final var row = rs.next();
          assertThat(row.<Object>getProperty("parent")).isNull();
          assertThat(row.<Object>getProperty("depth")).isNull();
          assertThat(row.<Object>getProperty("visible")).as("a name SQL can use still reads the store").isEqualTo("shared");
        }
      } finally {
        store.clear();
      }
    }
  }

  /**
   * Issue #8352: {@link Database#setWALFlush}, {@link Database#setUseWAL} and {@link Database#setAsyncFlush} changed
   * only the calling thread's transactions, so a multi-threaded application that asked for a flush on every commit got
   * it on one thread only. They are database-wide now, as their javadoc and neighbouring setters read; a thread keeps
   * a setting of its own through its transaction, which wins for that thread.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class WALSettingsScope extends TestHelper {

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE EDGE TYPE E");
    }

    @Test
    void databaseSettersReachEveryThread() throws Exception {
      // The issue's repro: set on one thread, committed from another. The same worker checks again after a second
      // change, so a transaction context it already created must follow too, not only a fresh one
      final CountDownLatch firstChecked = new CountDownLatch(1);
      final CountDownLatch changedAgain = new CountDownLatch(1);
      final AtomicReference<Throwable> failure = new AtomicReference<>();

      database.setWALFlush(WALFile.FlushType.YES_NOMETADATA);
      database.setUseWAL(false);
      database.setAsyncFlush(false);

      final Thread worker = new Thread(() -> {
        try {
          commitAndCheck(tx -> {
            assertThat(tx.getWALFlush()).isEqualTo(WALFile.FlushType.YES_NOMETADATA);
            assertThat(tx.isUseWAL()).isFalse();
            assertThat(tx.isAsyncFlush()).isFalse();
          });
          firstChecked.countDown();
          changedAgain.await();
          commitAndCheck(tx -> {
            assertThat(tx.getWALFlush()).isEqualTo(WALFile.FlushType.YES_FULL);
            assertThat(tx.isUseWAL()).isTrue();
            assertThat(tx.isAsyncFlush()).isTrue();
          });
        } catch (final Throwable t) {
          failure.set(t);
          firstChecked.countDown();
        }
      });
      worker.start();
      firstChecked.await();

      database.setWALFlush(WALFile.FlushType.YES_FULL);
      database.setUseWAL(true);
      database.setAsyncFlush(true);
      changedAgain.countDown();
      worker.join();
      assertThat(failure.get()).isNull();

      final TransactionContext own = ((DatabaseInternal) database).getTransaction();
      assertThat(own.getWALFlush()).isEqualTo(WALFile.FlushType.YES_FULL);
      assertThat(database.isAsyncFlush()).isTrue();
    }

    @Test
    void aThreadsOwnSettingWinsForThatThreadOnly() throws Exception {
      database.setWALFlush(WALFile.FlushType.YES_NOMETADATA);

      final TransactionContext own = ((DatabaseInternal) database).getTransaction();
      own.setWALFlush(WALFile.FlushType.NO);
      own.setUseWAL(false);
      own.setAsyncFlush(false);

      database.transaction(() -> database.newDocument("T").set("k", 1).save());
      assertThat(own.getWALFlush()).isEqualTo(WALFile.FlushType.NO);
      assertThat(own.isUseWAL()).isFalse();
      assertThat(own.isAsyncFlush()).isFalse();

      // The Database getter answers for the calling thread, its own setting included
      assertThat(database.isAsyncFlush()).isFalse();

      onAnotherThread(tx -> {
        assertThat(tx.getWALFlush()).isEqualTo(WALFile.FlushType.YES_NOMETADATA);
        assertThat(tx.isUseWAL()).isTrue();
        assertThat(tx.isAsyncFlush()).isTrue();
        assertThat(database.isAsyncFlush()).isTrue();
      });

      // Handing the thread back to the database's settings
      own.setWALFlush(null);
      own.setThreadUseWAL(null);
      assertThat(own.getWALFlush()).isEqualTo(WALFile.FlushType.YES_NOMETADATA);
      assertThat(own.isUseWAL()).isTrue();
    }

    @Test
    void aGraphBatchLeavesTheThreadFollowingTheDatabase() {
      // The batch relaxes the thread's WAL for the load and puts back what it found. Putting back the value in effect
      // instead of "follows the database" would pin the thread, deaf to a later database-wide change
      final TransactionContext own = ((DatabaseInternal) database).getTransaction();
      try (final GraphBatch batch = GraphBatch.builder(database).withWAL(false).withWALFlush(WALFile.FlushType.NO).build()) {
        database.begin();
        final MutableVertex v1 = batch.newVertex("V").save();
        final MutableVertex v2 = batch.newVertex("V").save();
        database.commit();
        batch.newEdge(v1.getIdentity(), "E", v2.getIdentity());
      }
      assertThat(own.getThreadUseWAL()).isNull();
      assertThat(own.getThreadWALFlush()).isNull();

      database.setWALFlush(WALFile.FlushType.YES_FULL);
      assertThat(own.getWALFlush()).isEqualTo(WALFile.FlushType.YES_FULL);
    }

    @Test
    void theAsyncExecutorLeavesTheDatabaseSettingsAlone() {
      // Its worker threads stamp their own policy on their transactions: that must not become the database's
      database.setWALFlush(WALFile.FlushType.YES_FULL);
      database.async().setTransactionSync(WALFile.FlushType.NO);
      database.async().setTransactionUseWAL(false);
      for (int i = 0; i < 50; i++)
        database.async().createRecord(database.newDocument("T").set("k", i), null);
      database.async().waitCompletion();

      final TransactionContext own = ((DatabaseInternal) database).getTransaction();
      assertThat(own.getWALFlush()).isEqualTo(WALFile.FlushType.YES_FULL);
      assertThat(own.isUseWAL()).isTrue();
      assertThat(database.countType("T", false)).isEqualTo(50);
    }

    private void commitAndCheck(final Consumer<TransactionContext> check) {
      database.transaction(() -> {
        database.newDocument("T").set("k", 0).save();
        check.accept(((DatabaseInternal) database).getTransaction());
      });
    }

    private void onAnotherThread(final Consumer<TransactionContext> check) throws Exception {
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Thread thread = new Thread(() -> {
        try {
          database.transaction(() -> {
            database.newDocument("T").set("k", 0).save();
            check.accept(((DatabaseInternal) database).getTransaction());
          });
        } catch (final Throwable t) {
          failure.set(t);
        }
      });
      thread.start();
      thread.join();
      assertThat(failure.get()).isNull();
    }
  }

  /**
   * Issue #9430: a database nothing replicates sizes its work against its own configuration. The replicated override
   * (the Raft wrapper answering the server's configuration) is covered in {@code ha-raft} by the
   * {@code Issue9430ServerOnly*HATest} classes.
   */
  @Nested
  class ReplicationConfigurationDefault extends TestHelper {

    @Test
    void aStandaloneDatabaseAnswersItsOwnConfiguration() {
      final DatabaseInternal db = (DatabaseInternal) database;
      assertThat(db.isReplicated()).isFalse();
      assertThat(db.getReplicationConfiguration()).isSameAs(db.getConfiguration());
      assertThat(db.getWrappedDatabaseInstance().getReplicationConfiguration()).isSameAs(db.getConfiguration());
    }
  }

  /**
   * Issue #9919: under Raft HA the save of {@code ALTER DATABASE} can be refused outright (a peer cannot decode the
   * configuration entry yet, this node left the configuration, ...), and the setting is then put back to the value the
   * command replaced. When a change to the SAME setting committed by another node was applied on this node inside that
   * window, the undo overwrote it with the stale previous value: this node ran on a value no other node held, and that
   * its own {@code configuration.json} did not hold.
   * <p>
   * The other node's apply is reproduced with the call the HA layer makes on every node,
   * {@link LocalDatabase#applyConfigurationChanges(String)}, from another thread as the Raft state machine runs it.
   */
  // A hang detector for the wait on the other node's apply, not a latency bound
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  @Nested
  class RefusedSettingUndo {
    private static final String DB_PATH  = "target/databases/Issue9919RefusedSettingUndoTest";
    private static final String KEY      = GlobalConfiguration.TYPE_DEFAULT_BUCKETS.getKey();
    private static final String FILE_KEY = KEY.substring(GlobalConfiguration.PREFIX.length());

    /** What another node committed for the same setting: what its apply installs on this node. */
    private static final String OTHER_NODE_CHANGE = new JSONObject().put("configuration", new JSONObject().put(FILE_KEY, 21))
        .put("removed", new JSONArray()).toString();

    private RefusingDatabase database;

    /**
     * Fires once, on the first read of {@link #KEY}'s value after it is armed: the read the command makes to capture the
     * value it replaces. Right after that read it starts the other node's apply on another thread and gives it the chance
     * to land before the command goes on - which it does unless the capture holds the lock the apply takes.
     */
    private static final class HookedConfiguration extends ContextConfiguration {
      private volatile RefusingDatabase database;
      private volatile boolean          armed;

      @Override
      public <T> T getValue(final String name, final T defaultValue) {
        final T value = super.getValue(name, defaultValue);
        if (armed && KEY.equals(name)) {
          armed = false;
          database.otherNodeApply = new Thread(() -> {
            try {
              database.applyConfigurationChanges(OTHER_NODE_CHANGE);
            } catch (final IOException e) {
              throw new RuntimeException(e);
            }
          });
          database.otherNodeApply.start();
          // Until the apply either landed or blocked: it blocks when the capture holds the apply lock, and then lands later,
          // inside the refused save
          final Thread apply = database.otherNodeApply;
          // The interrupt is how @Timeout ends a wait that never ends
          while (apply.isAlive() && apply.getState() != Thread.State.BLOCKED && !Thread.currentThread().isInterrupted())
            Thread.onSpinWait();
        }
        return value;
      }
    }

    /** A database whose configuration save is refused outright, as an HA save a peer cannot decode is. */
    private static final class RefusingDatabase extends LocalDatabase {
      private volatile boolean refuse;
      private volatile boolean otherNodeAppliesDuringTheSave;
      private volatile Thread  otherNodeApply;

      private RefusingDatabase(final String path, final HookedConfiguration configuration) {
        super(path, ComponentFile.MODE.READ_WRITE, configuration, null, new ConcurrentHashMap<>());
        configuration.database = this;
      }

      @Override
      public void saveConfiguration() throws IOException {
        if (!refuse) {
          super.saveConfiguration();
          return;
        }
        try {
          if (otherNodeApply != null)
            otherNodeApply.join();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        if (otherNodeAppliesDuringTheSave)
          applyConfigurationChanges(OTHER_NODE_CHANGE);
        throw new IllegalStateException("a peer cannot decode the configuration entry yet");
      }
    }

    private HookedConfiguration configuration;

    @BeforeEach
    void setUp() {
      FileUtils.deleteRecursively(new File(DB_PATH));
      PageManager.INSTANCE.acquire();
      configuration = new HookedConfiguration();
      database = new RefusingDatabase(DB_PATH, configuration);
      database.create();
      database.command("sql", "ALTER DATABASE `" + KEY + "` 3");
      database.refuse = true;
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    /**
     * The window the undo could not see: the other node's change is applied right after the command read the value it
     * replaces, and before it stores its own. The undo then put back the value the other node's change had already replaced.
     */
    @Test
    void anOtherNodeChangeAppliedBeforeTheCommandStoresItsValueSurvivesTheUndo() throws IOException {
      configuration.armed = true;

      assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `" + KEY + "` 13")).isInstanceOf(IllegalStateException.class);

      assertThat(configuration.armed).as("the hook fired").isFalse();
      assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS))
          .as("the node holds what the other node committed, as every node does").isEqualTo(21);
      assertThat(savedSettings().getInt(FILE_KEY)).isEqualTo(21);
      assertThat(database.pendingConfigurationChanges()).as("memory and file agree").isNull();
    }

    /** The other node's change is applied after the command stored its value, while the refused save is under way. */
    @Test
    void anOtherNodeChangeAppliedDuringTheRefusedSaveSurvivesTheUndo() throws IOException {
      database.otherNodeAppliesDuringTheSave = true;

      assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `" + KEY + "` 13")).isInstanceOf(IllegalStateException.class);

      assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).isEqualTo(21);
      assertThat(savedSettings().getInt(FILE_KEY)).isEqualTo(21);
      assertThat(database.pendingConfigurationChanges()).isNull();
    }

    /** Nothing else happened meanwhile: the refusal puts the previous value back, as before. */
    @Test
    void aRefusedChangeWithNothingElseMeanwhileIsUndone() throws IOException {
      assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `" + KEY + "` 13")).isInstanceOf(IllegalStateException.class);

      assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).isEqualTo(3);
      assertThat(database.pendingConfigurationChanges()).isNull();
    }

    /**
     * The undo puts the previous value back only while the setting still holds the value the refused change set: one
     * stored since by another writer stands, whether or not it reached the file yet.
     */
    @Test
    void theUndoLeavesAValueStoredSinceByAnotherWriter() {
      final LocalDatabase.SettingChange change = database.changeSetting(KEY, () -> database.getConfiguration().setValue(KEY, 13));
      assertThat(change.previousValue()).isEqualTo(3);
      assertThat(change.valueSet()).isEqualTo(13);

      // Another change to the same setting on this node, its save still under way
      database.getConfiguration().setValue(KEY, 17);

      database.undoSettingChange(change);

      assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).isEqualTo(17);
    }

    /** A setting not set before the refused change goes back to "not set", so it falls back to its default. */
    @Test
    void theUndoOfASettingNotSetBeforeRemovesIt() {
      final LocalDatabase.SettingChange change = database.changeSetting(GlobalConfiguration.TX_RETRIES.getKey(),
          () -> database.getConfiguration().setValue(GlobalConfiguration.TX_RETRIES, 7));
      assertThat(change.previousValue()).isNull();

      database.undoSettingChange(change);

      assertThat(database.getConfiguration().getContextKeys()).doesNotContain(GlobalConfiguration.TX_RETRIES.getKey());
    }

    private JSONObject savedSettings() throws IOException {
      return new JSONObject(FileUtils.readFileAsString(new File(DB_PATH, LocalDatabase.CONFIGURATION_FILE_NAME)))
          .getJSONObject("configuration");
    }
  }

  /**
   * Issue #9949: {@link LocalDatabase#changeSetting(String, Runnable)} re-derives the per-database state from the value the
   * change stored. When that re-derivation fails, the previous value goes back - a setting that was not set is removed, not
   * stored as an explicit entry - so the configuration never names a value the database is not running with.
   */
  @Nested
  class ChangeSettingRollback extends TestHelper {
    private static final String KEY = GlobalConfiguration.DATE_IMPLEMENTATION.getKey();

    @Test
    void aFailedReDerivationOfASettingThatWasNotSetLeavesItUnset() {
      final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
      assertThat(local.getConfiguration().hasValue(KEY)).isFalse();

      // stored raw, so only the re-derivation finds the class cannot be loaded
      assertThatThrownBy(() -> local.changeSetting(KEY, () -> local.getConfiguration().setValue(KEY, "com.example.NoSuchDate")))
          .isInstanceOf(RuntimeException.class);

      assertThat(local.getConfiguration().hasValue(KEY)).isFalse();
      assertThat(local.getSerializer().getDateImplementation()).isEqualTo(LocalDate.class);
    }

    @Test
    void aFailedReDerivationPutsBackTheValueSetBefore() {
      final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
      local.changeSetting(KEY, () -> local.getConfiguration().setValue(KEY, Date.class.getName()));
      assertThat(local.getSerializer().getDateImplementation()).isEqualTo(Date.class);

      assertThatThrownBy(() -> local.changeSetting(KEY, () -> local.getConfiguration().setValue(KEY, "com.example.NoSuchDate")))
          .isInstanceOf(RuntimeException.class);

      assertThat(local.getConfiguration().getValue(KEY, null).toString()).isEqualTo(Date.class.getName());
      assertThat(local.getSerializer().getDateImplementation()).isEqualTo(Date.class);
    }
  }

  /**
   * Issue #9951: two changes to the SAME setting on the SAME node, both refused. The first change (A) stores V1 over P,
   * the second (B) stores V2 capturing V1 as the value it replaced. A is refused while B is in flight, so its undo leaves
   * V2; then B is refused and its undo put back V1 - the value of a change that was itself refused, held by no other node
   * and not by {@code configuration.json}. The undo now knows whether the change it would restore was itself refused, and
   * then settles on what {@code configuration.json} holds instead.
   */
  // A hang detector for the latches that order the two commands, not a latency bound
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  @Nested
  class SameSettingRefusedUndo {
    private static final String DB_PATH  = "target/databases/Issue9951SameSettingRefusedUndoTest";
    private static final String KEY      = GlobalConfiguration.TYPE_DEFAULT_BUCKETS.getKey();
    private static final String FILE_KEY = KEY.substring(GlobalConfiguration.PREFIX.length());

    /**
     * A database whose configuration save is refused outright, as an HA save a peer cannot decode is. With
     * {@link #interleave} the first refused save waits until a second one started, and the second is refused only once
     * the first command finished its undo: A refused first, then B, as in the issue.
     */
    private static final class RefusingDatabase extends LocalDatabase {
      private volatile boolean        refuse;
      private volatile boolean        interleave;
      private final    AtomicInteger  refusedSaves = new AtomicInteger();
      private final    CountDownLatch firstInSave  = new CountDownLatch(1);
      private final    CountDownLatch secondInSave = new CountDownLatch(1);
      private volatile Thread         firstCommand;

      private RefusingDatabase(final String path) {
        super(path, ComponentFile.MODE.READ_WRITE, new ContextConfiguration(), null, new ConcurrentHashMap<>());
      }

      @Override
      public void saveConfiguration() throws IOException {
        if (!refuse) {
          super.saveConfiguration();
          return;
        }
        if (interleave) {
          try {
            if (refusedSaves.incrementAndGet() == 1) {
              firstInSave.countDown();
              secondInSave.await();
            } else {
              secondInSave.countDown();
              // Refused only after the first command is refused and has run its undo
              firstCommand.join();
            }
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        throw new IllegalStateException("a peer cannot decode the configuration entry yet");
      }
    }

    private RefusingDatabase database;

    @BeforeEach
    void setUp() {
      FileUtils.deleteRecursively(new File(DB_PATH));
      PageManager.INSTANCE.acquire();
      database = new RefusingDatabase(DB_PATH);
      database.create();
      database.command("sql", "ALTER DATABASE `" + KEY + "` 3");
      database.refuse = true;
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    /** The issue's sequence through ALTER DATABASE: A stores 13, B stores 17, A is refused, then B is refused. */
    @Test
    void twoRefusedAlterDatabaseOnTheSameSettingLeaveTheSavedValue() throws Exception {
      database.interleave = true;

      final AtomicReference<Throwable> firstFailure = new AtomicReference<>();
      final Thread first = new Thread(() -> {
        try {
          database.command("sql", "ALTER DATABASE `" + KEY + "` 13");
        } catch (final Throwable t) {
          firstFailure.set(t);
        }
      });
      database.firstCommand = first;
      first.start();
      database.firstInSave.await();

      assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `" + KEY + "` 17")).isInstanceOf(IllegalStateException.class);
      first.join();
      assertThat(firstFailure.get()).isInstanceOf(IllegalStateException.class);

      assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS))
          .as("both changes were refused: the node is back on the value configuration.json holds").isEqualTo(3);
      assertThat(savedSettings().getInt(FILE_KEY)).isEqualTo(3);
      assertThat(database.pendingConfigurationChanges()).as("memory and file agree").isNull();
    }

    /** A refused while B is in flight: A's undo leaves B's value; B refused: back to the saved value, not A's. */
    @Test
    void theUndoOfAChangeThatReplacedARefusedOneFallsBackToTheSavedValue() throws IOException {
      final LocalDatabase.SettingChange a = database.changeSetting(KEY, () -> database.getConfiguration().setValue(KEY, 13));
      final LocalDatabase.SettingChange b = database.changeSetting(KEY, () -> database.getConfiguration().setValue(KEY, 17));
      assertThat(b.previousValue()).isEqualTo(13);

      database.undoSettingChange(a);
      assertThat(valueInForce()).as("B is still in flight: its value stands").isEqualTo(17);

      database.undoSettingChange(b);
      assertThat(valueInForce()).as("13 belongs to a refused change: not put back").isEqualTo(3);
      assertThat(database.pendingConfigurationChanges()).isNull();
    }

    /** B refused first: A is still in flight, so its value comes back; then A refused: back to the value A replaced. */
    @Test
    void refusedInTheOrderTheyWereMadeEachUndoPutsBackThePreviousValue() throws IOException {
      final LocalDatabase.SettingChange a = database.changeSetting(KEY, () -> database.getConfiguration().setValue(KEY, 13));
      final LocalDatabase.SettingChange b = database.changeSetting(KEY, () -> database.getConfiguration().setValue(KEY, 17));

      database.undoSettingChange(b);
      assertThat(valueInForce()).as("A is still in flight: its value comes back").isEqualTo(13);

      database.undoSettingChange(a);
      assertThat(valueInForce()).isEqualTo(3);
      assertThat(database.pendingConfigurationChanges()).isNull();
    }

    /**
     * The second ambiguity in the issue: B stores the SAME value A stored, its save under way. Comparing values could not
     * tell that from "untouched", so A's undo reverted B's value, which no file held yet; B's own save would then have
     * shipped the reverted value.
     */
    @Test
    void theUndoLeavesTheSameValueStoredSinceByAnotherChange() throws IOException {
      final LocalDatabase.SettingChange a = database.changeSetting(KEY, () -> database.getConfiguration().setValue(KEY, 13));
      final LocalDatabase.SettingChange b = database.changeSetting(KEY, () -> database.getConfiguration().setValue(KEY, 13));

      database.undoSettingChange(a);
      assertThat(valueInForce()).as("B stored 13 too and is still in flight").isEqualTo(13);

      database.undoSettingChange(b);
      assertThat(valueInForce()).as("both refused").isEqualTo(3);
      assertThat(database.pendingConfigurationChanges()).isNull();
    }

    /** A change superseded by a replicated apply that was then refused is not put back by a later undo. */
    @Test
    void anApplySinceTheChangeLeavesItsValueInForce() throws IOException {
      final LocalDatabase.SettingChange a = database.changeSetting(KEY, () -> database.getConfiguration().setValue(KEY, 13));
      // Another node committed the same value: memory and file hold 13 now
      database.applyConfigurationChanges(
          new JSONObject().put("configuration", new JSONObject().put(FILE_KEY, 13)).put("removed", new JSONArray()).toString());

      database.undoSettingChange(a);

      assertThat(valueInForce()).isEqualTo(13);
      assertThat(savedSettings().getInt(FILE_KEY)).isEqualTo(13);
      assertThat(database.pendingConfigurationChanges()).isNull();
    }

    /**
     * B fails inside the change itself and puts A's value back: A, still in flight, is the latest change again, so A's
     * refusal still undoes it.
     */
    @Test
    void aChangeThatFailsInsideLeavesTheEarlierChangeUndoable() throws IOException {
      final LocalDatabase.SettingChange a = database.changeSetting(KEY, () -> database.getConfiguration().setValue(KEY, 13));
      assertThatThrownBy(() -> database.changeSetting(KEY, () -> {
        database.getConfiguration().setValue(KEY, 17);
        // What putInForce() does when the side effect of the new value fails
        database.restoreSetting(KEY, 13);
        throw new IllegalArgumentException("side effect failed");
      })).isInstanceOf(IllegalArgumentException.class);
      assertThat(valueInForce()).isEqualTo(13);

      database.undoSettingChange(a);

      assertThat(valueInForce()).isEqualTo(3);
      assertThat(database.pendingConfigurationChanges()).isNull();
    }

    private int valueInForce() {
      return database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS);
    }

    private JSONObject savedSettings() throws IOException {
      return new JSONObject(FileUtils.readFileAsString(new File(DB_PATH, LocalDatabase.CONFIGURATION_FILE_NAME)))
          .getJSONObject("configuration");
    }
  }

  /**
   * Issue #9959: under Raft HA the entry a configuration change replicates was built from the WHOLE difference between
   * the in-memory configuration and {@code configuration.json}. A value another statement on the same node had put in
   * force and not submitted yet was therefore shipped with this statement's entry, and committed cluster-wide even when
   * that other statement's own save was then refused and its caller told nothing had changed.
   * <p>
   * These tests pin the CONTENT of the entry, not the final value of a setting: the entry a statement submits names the
   * setting it changed and nothing else. The other statement is the in-memory store it makes before its save, done here
   * directly on the configuration.
   */
  @Nested
  class PerSettingConfigurationEntry {
    private static final String DB_PATH = "target/databases/Issue9959PerSettingConfigurationEntryTest";

    /** What the Raft HA wrapper would submit for the setting saved, captured at the save. */
    private static final class RecordingDatabase extends LocalDatabase {
      private volatile String savedKey;
      private volatile String submittedEntry;

      private RecordingDatabase(final String path) {
        super(path, ComponentFile.MODE.READ_WRITE, new ContextConfiguration(), null, new ConcurrentHashMap<>());
      }

      @Override
      public void saveConfiguration(final SettingChange change) throws IOException {
        savedKey = change.key();
        // Before the save, as the HA wrapper computes it: the save writes the file, after which nothing is pending
        submittedEntry = pendingConfigurationChange(change);
        super.saveConfiguration(change);
      }
    }

    private RecordingDatabase database;

    @BeforeEach
    void setUp() {
      FileUtils.deleteRecursively(new File(DB_PATH));
      PageManager.INSTANCE.acquire();
      database = new RecordingDatabase(DB_PATH);
      database.create();
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    @Test
    void theChangeToOneSettingLeavesOutAnotherPendingOne() throws IOException {
      // Another statement's setting, in force and not submitted yet
      database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 2);
      database.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 5);

      final JSONObject entry = new JSONObject(database.pendingConfigurationChange(GlobalConfiguration.TYPE_DEFAULT_BUCKETS.getKey()));
      assertThat(entry.getJSONObject("configuration").keySet()).containsExactly("typeDefaultBuckets");
      assertThat(entry.getJSONObject("configuration").getInt("typeDefaultBuckets")).isEqualTo(5);
      assertThat(entry.getJSONArray("removed").length()).isZero();

      // The whole difference, which the entry used to be, holds both
      assertThat(new JSONObject(database.pendingConfigurationChanges()).getJSONObject("configuration").keySet())
          .containsExactlyInAnyOrder("typeDefaultBuckets", "txWalFlush");
    }

    @Test
    void aDeclaredSettingIsFoundHoweverTheKeyIsSpelled() throws IOException {
      database.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 5);

      for (final String spelling : new String[] { "typeDefaultBuckets", "ARCADEDB.TYPEDEFAULTBUCKETS" })
        assertThat(new JSONObject(database.pendingConfigurationChange(spelling)).getJSONObject("configuration").keySet())
            .as(spelling).containsExactly("typeDefaultBuckets");
    }

    @Test
    void aSettingFileAndMemoryAgreeOnHasNoChange() throws IOException {
      database.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 5);
      database.saveConfiguration();
      database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 2);

      assertThat(database.pendingConfigurationChange(GlobalConfiguration.TYPE_DEFAULT_BUCKETS.getKey()))
          .as("only another setting is pending, and it is not this one's to ship").isNull();
    }

    @Test
    void aRemovedSettingIsTheOnlyOneNamed() throws IOException {
      database.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 5);
      database.getConfiguration().setValue(GlobalConfiguration.TX_RETRIES, 9);
      database.saveConfiguration();

      database.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, null);
      database.getConfiguration().setValue(GlobalConfiguration.TX_RETRIES, null);
      database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 2);

      final JSONObject entry = new JSONObject(database.pendingConfigurationChange(GlobalConfiguration.TYPE_DEFAULT_BUCKETS.getKey()));
      assertThat(entry.getJSONObject("configuration").keySet()).isEmpty();
      assertThat(entry.getJSONArray("removed").toList()).containsExactly("typeDefaultBuckets");
    }

    @Test
    void theFileNameOfASettingKeepsAKeyWithoutThePrefixWhole() {
      assertThat(ContextConfiguration.jsonKey("arcadedb.typeDefaultBuckets")).isEqualTo("typeDefaultBuckets");
      assertThat(ContextConfiguration.jsonKey("custom")).isEqualTo("custom");
    }

    /** The writer: ALTER DATABASE saves its own setting by key, so the entry it submits names that setting alone. */
    @Test
    void alterDatabaseSubmitsItsOwnSettingOnly() {
      database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 2);

      database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 4");

      assertThat(database.savedKey).isEqualTo(GlobalConfiguration.TYPE_DEFAULT_BUCKETS.getKey());
      assertThat(database.submittedEntry).isNotNull();
      final JSONObject entry = new JSONObject(database.submittedEntry);
      assertThat(entry.getJSONObject("configuration").keySet()).containsExactly("typeDefaultBuckets");
      assertThat(entry.getJSONObject("configuration").getInt("typeDefaultBuckets")).isEqualTo(4);
      assertThat(entry.getJSONArray("removed").length()).isZero();
    }
  }

  /**
   * Issue #9976: a change of {@code arcadedb.dateImplementation} or {@code arcadedb.dateTimeImplementation} checks the patterns
   * of exactly the types whose held class it changes, through {@link DocumentValidator#heldClassUnder}. That mapping has to
   * agree with {@link Type#getJavaImplementation} on every type: a type added later and left out would have its patterns
   * silently skipped by the check.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class HeldClassMapping extends TestHelper {

    @Test
    void theMappingAgreesWithTheSerializerOnEveryType() {
      final BinarySerializer serializer = ((DatabaseInternal) database).getSerializer();
      final Class<?> previousDate = serializer.getDateImplementation();
      final Class<?> previousDateTime = serializer.getDateTimeImplementation();
      try {
        for (final Class<?> dateImplementation : List.of(LocalDate.class, Date.class, Calendar.class, LocalDateTime.class))
          for (final Class<?> dateTimeImplementation : List.of(LocalDateTime.class, Date.class, Calendar.class, Instant.class,
              ZonedDateTime.class)) {
            serializer.setDateImplementation(dateImplementation);
            serializer.setDateTimeImplementation(dateTimeImplementation);
            for (final Type type : Type.values()) {
              final Class<?> mapped = DocumentValidator.heldClassUnder(type, dateImplementation, dateTimeImplementation);
              if (mapped != null)
                assertThat(mapped).as("%s under %s / %s", type, dateImplementation.getSimpleName(),
                    dateTimeImplementation.getSimpleName()).isEqualTo(type.getJavaImplementation(database));
              else
                // a type the mapping leaves out is one the two settings do not move
                assertThat(type.getJavaImplementation(database)).as("%s, left out of the mapping", type)
                    .isEqualTo(type.getJavaImplementation(null));
            }
          }
      } finally {
        serializer.setDateImplementation(previousDate);
        serializer.setDateTimeImplementation(previousDateTime);
      }
    }
  }

  /**
   * Issue #9763: {@code ALTER DATABASE} undoes its in-memory change when the save fails, because under Raft HA the save
   * is what replicates the setting. It undoes that one setting only, so a change to another setting that landed while
   * the save was in flight survives. A save that fails with {@link TransactionCommittedRemotelyException} is not undone
   * at all: the change IS committed cluster-wide and every node installs it, this one included, so undoing it here
   * would leave this node alone on the previous value.
   */
  @Nested
  class SaveFailureRollback {
    private static final String DB_PATH = "target/databases/Issue9763SaveFailureRollbackTest";

    private FailingSaveDatabase database;

    /** A database whose configuration save fails the way the HA save can, optionally after a concurrent change. */
    private static final class FailingSaveDatabase extends LocalDatabase {
      private volatile RuntimeException saveFailure;
      private volatile Runnable         duringSave;

      private FailingSaveDatabase(final String path) {
        super(path, ComponentFile.MODE.READ_WRITE, new ContextConfiguration(), null, new ConcurrentHashMap<>());
      }

      @Override
      public void saveConfiguration() throws IOException {
        if (duringSave != null)
          duringSave.run();
        if (saveFailure != null)
          throw saveFailure;
        super.saveConfiguration();
      }

      /** What another save made on this node does when it ships, and so commits, this change too. */
      private void saveForReal() {
        try {
          super.saveConfiguration();
        } catch (final IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }

    @BeforeEach
    void setUp() {
      FileUtils.deleteRecursively(new File(DB_PATH));
      PageManager.INSTANCE.acquire();
      database = new FailingSaveDatabase(DB_PATH);
      database.create();
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    @Test
    void aSettingCommittedRemotelyStaysInForce() {
      database.saveFailure = new TransactionCommittedRemotelyException("committed by the majority, not confirmed by every peer");

      assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 13"))
          .isInstanceOf(TransactionCommittedRemotelyException.class);

      assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).isEqualTo(13);
    }

    /**
     * Possibly committed: the setting settles on what configuration.json holds, which the apply writes if the change
     * commits (issue #9904). Nothing was applied here, so the previous value is back and nothing stays pending for the
     * next change made on this node to carry along. UnknownSaveOutcome covers the other orders of events.
     */
    @Test
    void aSettingWhoseOutcomeIsUnknownSettlesOnTheSavedConfiguration() throws Exception {
      database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 3");
      database.saveFailure = new TransactionOutcomeUnknownException("dispatched to Raft, then the wait gave up");

      assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 13"))
          .isInstanceOf(TransactionOutcomeUnknownException.class);

      assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).isEqualTo(3);
      assertThat(database.pendingConfigurationChanges()).isNull();
    }

    @Test
    void aFailedSaveUndoesItsOwnSettingOnlyAndKeepsAConcurrentChange() {
      database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 3");
      database.saveFailure = new IllegalStateException("refused: a peer cannot decode the entry yet");
      // Another setting changes while the save is in flight, as a configuration entry replicated by another node would
      database.duringSave = () -> database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 2);

      assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 13"))
          .isInstanceOf(IllegalStateException.class);

      assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS))
          .as("the failed change is undone").isEqualTo(3);
      assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_WAL_FLUSH))
          .as("the concurrent change to another setting is kept").isEqualTo(2);
    }

    /**
     * Under HA a concurrent change made on this node ships every setting memory holds that the file does not, so it can
     * commit this change before this save fails. The value is then in the file, committed, and must stay.
     */
    @Test
    void aFailedSaveKeepsAValueAnotherSaveAlreadyCommitted() {
      database.saveFailure = new IllegalStateException("refused: a peer cannot decode the entry yet");
      database.duringSave = database::saveForReal;

      assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 13"))
          .isInstanceOf(IllegalStateException.class);

      assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).isEqualTo(13);
    }

    @Test
    void aFailedSaveOfASettingThatWasNotSetPutsItBackToItsDefault() {
      database.saveFailure = new IllegalStateException("refused: a peer cannot decode the entry yet");

      assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 13"))
          .isInstanceOf(IllegalStateException.class);

      assertThat(database.getConfiguration().getContextKeys()).doesNotContain(GlobalConfiguration.TYPE_DEFAULT_BUCKETS.getKey());
    }
  }

  /**
   * Issue #9904: under Raft HA the save of {@code ALTER DATABASE} replicates the change, and the replication can give up
   * waiting on an entry it already dispatched ({@link TransactionOutcomeUnknownException}): the change may still commit,
   * and this node applies it like every other node, writing its own {@code configuration.json}. Putting the previous
   * value back then raced that apply: run after it, it left this node alone on the old value until its next restart.
   * <p>
   * The setting now settles on what this node's {@code configuration.json} holds - which the apply writes - so each
   * order of events ends where the cluster does. The apply is reproduced here with the same two calls the HA layer
   * makes: {@link LocalDatabase#pendingConfigurationChanges()} on the submitting side and
   * {@link LocalDatabase#applyConfigurationChanges(String)} on every node.
   */
  @Nested
  class UnknownSaveOutcome {
    private static final String DB_PATH = "target/databases/Issue9904UnknownSaveOutcomeTest";

    private UnknownOutcomeDatabase database;

    /**
     * A database whose configuration save behaves like the HA save that dispatched its entry and then stopped waiting:
     * the changes are captured as the HA layer would submit them, optionally applied, and the save reports an unknown
     * outcome.
     */
    private static final class UnknownOutcomeDatabase extends LocalDatabase {
      private volatile boolean                 unknownOutcome;
      private volatile boolean                 appliedBeforeTheWaitGivesUp;
      private final    AtomicReference<String> submitted = new AtomicReference<>();

      private UnknownOutcomeDatabase(final String path) {
        super(path, ComponentFile.MODE.READ_WRITE, new ContextConfiguration(), null, new ConcurrentHashMap<>());
      }

      @Override
      public void saveConfiguration() throws IOException {
        if (!unknownOutcome) {
          super.saveConfiguration();
          return;
        }
        final String changes = pendingConfigurationChanges();
        submitted.set(changes);
        if (appliedBeforeTheWaitGivesUp)
          applyConfigurationChanges(changes);
        throw new TransactionOutcomeUnknownException("the entry was dispatched, then the wait for its quorum gave up");
      }
    }

    @BeforeEach
    void setUp() {
      FileUtils.deleteRecursively(new File(DB_PATH));
      PageManager.INSTANCE.acquire();
      database = new UnknownOutcomeDatabase(DB_PATH);
      database.create();
      database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 3");
      database.unknownOutcome = true;
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    /** The case the issue names: the apply ran before the caller handled the failure. The old code undid it. */
    @Test
    void aChangeAppliedBeforeTheWaitGaveUpStaysInForce() throws IOException {
      database.appliedBeforeTheWaitGivesUp = true;

      assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 13"))
          .isInstanceOf(TransactionOutcomeUnknownException.class);

      assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS))
          .as("the node keeps what it applied, as every other node does").isEqualTo(13);
      assertThat(savedSettings().getInt("typeDefaultBuckets")).isEqualTo(13);
      assertThat(database.pendingConfigurationChanges()).as("memory and file agree").isNull();
    }

    /** The per-database side effect follows the setting, not only the configuration map. */
    @Test
    void aDateImplementationAppliedBeforeTheWaitGaveUpStaysInForceOnTheSerializer() {
      database.command("sql", "CREATE DOCUMENT TYPE D");
      database.command("sql", "CREATE PROPERTY D.day DATE");
      database.transaction(() -> database.newDocument("D").set("day", LocalDate.of(2026, 10, 13)).save());
      assertThat(database.getSerializer().getDateImplementation()).as("precondition").isNotEqualTo(Date.class);
      database.appliedBeforeTheWaitGivesUp = true;

      assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.dateImplementation` 'java.util.Date'"))
          .isInstanceOf(TransactionOutcomeUnknownException.class);

      assertThat(database.getSerializer().getDateImplementation()).isEqualTo(Date.class);
      assertThat(database.query("sql", "SELECT day FROM D").next().<Object>getProperty("day")).isInstanceOf(Date.class);
    }

    /** Not applied yet: the previous value is back now, and the apply installs the new one when the entry commits. */
    @Test
    void aChangeCommittedAfterTheWaitGaveUpIsInstalledByItsApply() throws IOException {
      assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 13"))
          .isInstanceOf(TransactionOutcomeUnknownException.class);

      assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS))
          .as("nothing installed yet").isEqualTo(3);

      // The entry commits afterwards: this node applies it like every other node
      database.applyConfigurationChanges(database.submitted.get());

      assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).isEqualTo(13);
      assertThat(savedSettings().getInt("typeDefaultBuckets")).isEqualTo(13);
    }

    /**
     * Never committed: the node is back on the value every other node holds, and holds nothing its file does not, so the
     * next change elsewhere does not carry this one along.
     */
    @Test
    void aChangeThatNeverCommitsLeavesNothingBehind() throws IOException {
      assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 13"))
          .isInstanceOf(TransactionOutcomeUnknownException.class);

      assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).isEqualTo(3);
      assertThat(database.pendingConfigurationChanges()).isNull();
    }

    /** A setting the file has never held falls back to its default, as an undo to "not set" does. */
    @Test
    void aSettingTheFileDoesNotNameFallsBackToItsDefault() {
      assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.txRetries` 7"))
          .isInstanceOf(TransactionOutcomeUnknownException.class);

      assertThat(database.getConfiguration().getContextKeys()).doesNotContain(GlobalConfiguration.TX_RETRIES.getKey());
    }

    /**
     * A setting no {@link GlobalConfiguration} declares ({@code set database setting} stores one as given) is settled on
     * the file too. Reading it back through {@link ContextConfiguration#fromJSON}, which skips undeclared keys, reset it
     * although the file holds it, and the next change on this node then shipped it as a removal.
     */
    @Test
    void anUndeclaredSettingAppliedBeforeTheWaitGaveUpStaysInForce() throws IOException {
      final String key = GlobalConfiguration.PREFIX + "issue9904Custom";
      database.appliedBeforeTheWaitGivesUp = true;
      database.getConfiguration().setValue(key, "v1");

      assertThatThrownBy(database::saveConfiguration).isInstanceOf(TransactionOutcomeUnknownException.class);
      database.restoreSettingFromSavedConfiguration(key);

      final Object inForce = database.getConfiguration().getValue(key, null);
      assertThat(inForce).isEqualTo("v1");
      assertThat(savedSettings().getString("issue9904Custom")).isEqualTo("v1");
      assertThat(database.pendingConfigurationChanges()).as("memory and file agree, so nothing ships as a removal").isNull();
    }

    private JSONObject savedSettings() throws IOException {
      return new JSONObject(FileUtils.readFileAsString(new File(DB_PATH, LocalDatabase.CONFIGURATION_FILE_NAME)))
          .getJSONObject("configuration");
    }
  }
}
