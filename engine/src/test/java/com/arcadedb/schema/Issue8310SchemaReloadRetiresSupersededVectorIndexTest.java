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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8310.
 * <p>
 * A schema reload builds a new instance of every index it re-reads, publishes it, and until this fix left the
 * instance it replaced exactly as it was: nothing ever stopped its background work, because
 * {@code LocalDatabase} releases only the indexes the schema still lists on close and drop, and the superseded one
 * is no longer listed. An {@code LSMVectorIndex} carries an inactivity rebuild timer - one daemon {@code Timer}
 * thread per instance - armed by any write, so the retired instance woke up once the window expired and ran a full
 * graph build of its own: against the live database, contending for the JVM-wide {@code REBUILD_SEMAPHORE} and
 * persisting a graph file the NEW instance also owns, or, when the database had been dropped in between, against a
 * closed one. The CI log of issue #8178 shows the second shape: {@code Issue7213SchemaLoadPublicationBarrierTest}
 * passed, and fifteen seconds later - the default inactivity window - a timer from one of its reloads failed with
 * {@code DatabaseIsClosedException} on a database the test had already dropped.
 * <p>
 * The same leak runs on an HA follower on every schema entry it applies, since each one reloads the schema.
 */
class Issue8310SchemaReloadRetiresSupersededVectorIndexTest extends TestHelper {

  private static final String TYPE_NAME           = "Issue8310Doc";
  private static final String INDEX_NAME          = TYPE_NAME + "[embedding]";
  private static final String TIMER_THREAD_PREFIX = "VectorIndex-InactivityTimer-";

  @Override
  protected void beginTest() {
    // Long enough that the timer cannot fire while a test runs: what is asserted is that it is CANCELLED, not that
    // it has not fired yet. Set on this database only, not JVM-wide, so no other test class is affected.
    ((DatabaseInternal) database).getConfiguration()
        .setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 60_000);

    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME);
      database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".name STRING");
      database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON " + TYPE_NAME + " (embedding) LSM_VECTOR "
          + "METADATA { dimensions: 3, similarity: 'COSINE', idPropertyName: 'name' }");
    });

    // A handful of writes, far below the rebuild threshold: this is what arms the inactivity timer.
    database.transaction(() -> {
      database.newDocument(TYPE_NAME).set("name", "a").set("embedding", new float[] { 1.0f, 0.0f, 0.0f }).save();
      database.newDocument(TYPE_NAME).set("name", "b").set("embedding", new float[] { 0.0f, 1.0f, 0.0f }).save();
      database.newDocument(TYPE_NAME).set("name", "c").set("embedding", new float[] { 0.0f, 0.0f, 1.0f }).save();
    });
  }

  @Test
  void aFullLoadRetiresTheVectorIndexItSupersedes() throws Exception {
    final LocalSchema schema = schema();
    final IndexInternal before = bucketLevelVectorIndex(schema);
    assertThat(timerThreadsOf(before)).as("the fixture must arm the inactivity timer it then expects gone").hasSize(1);

    schema.load(ComponentFile.MODE.READ_WRITE, true);

    assertSupersededAndRetired(schema, before);
  }

  /**
   * The path an HA follower takes for every applied schema entry (#6988), where a touched index file is re-read into
   * a replacement instance while everything else keeps its instance.
   */
  @Test
  void anIncrementalRefreshRetiresTheVectorIndexItReplaces() throws Exception {
    final LocalSchema schema = schema();
    final IndexInternal before = bucketLevelVectorIndex(schema);
    assertThat(timerThreadsOf(before)).as("the fixture must arm the inactivity timer it then expects gone").hasSize(1);

    assertThat(schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of(before.getFileIds().getFirst())))
        .as("the refresh must have been expressible incrementally")
        .isTrue();

    assertSupersededAndRetired(schema, before);
  }

  /**
   * The other half of the contract: an incremental refresh that does not touch the index keeps serving the SAME
   * instance, and that one must be left running. Releasing by "was published before" rather than by "is not
   * published any more" would stop the live index's timer here and silently end its background rebuilds.
   */
  @Test
  void aRefreshThatKeepsTheInstanceLeavesItRunning() throws Exception {
    final LocalSchema schema = schema();
    final IndexInternal before = bucketLevelVectorIndex(schema);

    assertThat(schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of())).isTrue();

    assertThat(bucketLevelVectorIndex(schema)).as("an untouched index keeps its instance").isSameAs(before);
    assertThat(before.isValid()).as("the instance still serving must not be released").isTrue();
    assertThat(timerThreadsOf(before)).as("and its inactivity timer must still be armed").hasSize(1);
    assertThat(neighbors()).containsExactly("a");
  }

  /**
   * Cancelling the timer is half of it: a write that resolved the retired instance before the swap and commits after
   * it would arm a fresh one. Driven on the live instance, retired directly, because that is the only way to route
   * an ordinary committed write to an instance that has been retired.
   */
  @Test
  void aWriteToARetiredInstanceDoesNotArmItsTimerAgain() {
    final IndexInternal index = bucketLevelVectorIndex(schema());
    index.onSuperseded();
    Awaitility.await("the retired instance's inactivity timer thread exits")
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(20))
        .untilAsserted(() -> assertThat(timerThreadsOf(index)).isEmpty());

    database.transaction(() ->
        database.newDocument(TYPE_NAME).set("name", "d").set("embedding", new float[] { 1.0f, 1.0f, 0.0f }).save());

    assertThat(timerThreadsOf(index)).as("a retired instance must not arm the timer again").isEmpty();
    assertThat(neighbors()).as("and the write itself still lands").containsExactly("a");
  }

  /**
   * The same leak one door over: a dropped index leaves the schema too, and {@code drop()} marked it invalid but
   * left the timer armed, so it fired later and started a build over files the drop had just deleted.
   */
  @Test
  void droppingTheIndexStopsItsTimer() {
    final IndexInternal index = bucketLevelVectorIndex(schema());
    assertThat(timerThreadsOf(index)).as("the fixture must arm the inactivity timer it then expects gone").hasSize(1);

    database.command("sql", "DROP INDEX `" + INDEX_NAME + "`");

    Awaitility.await("the dropped index's inactivity timer thread exits")
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(20))
        .untilAsserted(() -> assertThat(timerThreadsOf(index)).isEmpty());
  }

  /** {@code REBUILD INDEX} replaces the instance by dropping the old one, so it must leave no timer behind either. */
  @Test
  void rebuildingTheIndexStopsTheTimerOfTheInstanceItReplaces() {
    final IndexInternal before = bucketLevelVectorIndex(schema());
    assertThat(timerThreadsOf(before)).as("the fixture must arm the inactivity timer it then expects gone").hasSize(1);

    database.command("sql", "REBUILD INDEX `" + INDEX_NAME + "`");

    assertThat(bucketLevelVectorIndex(schema())).as("the rebuild must have replaced the instance").isNotSameAs(before);
    Awaitility.await("the replaced index's inactivity timer thread exits")
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(20))
        .untilAsserted(() -> assertThat(timerThreadsOf(before)).isEmpty());
    assertThat(neighbors()).as("the rebuilt index answers").containsExactly("a");
  }

  private void assertSupersededAndRetired(final LocalSchema schema, final IndexInternal before) {
    final IndexInternal after = bucketLevelVectorIndex(schema);
    assertThat(after).as("the reload must have replaced the instance").isNotSameAs(before);
    assertThat(after.isValid()).as("the instance now published is live").isTrue();

    // Retired, not invalidated: a query that resolved it just before the swap may still be running on it.
    assertThat(before.isValid()).as("the superseded instance must keep answering an in-flight query").isTrue();
    // A cancelled Timer's thread exits asynchronously, so give it a bounded window. The published instance has the
    // same name, and therefore the same thread name, but has taken no write since the reload and armed nothing.
    Awaitility.await("the superseded instance's inactivity timer thread exits")
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(20))
        .untilAsserted(() -> assertThat(timerThreadsOf(before)).isEmpty());

    assertThat(neighbors()).as("the published instance still answers").containsExactly("a");
  }

  private static List<Thread> timerThreadsOf(final IndexInternal index) {
    final String name = TIMER_THREAD_PREFIX + index.getName();
    final List<Thread> result = new ArrayList<>();
    for (final Thread t : Thread.getAllStackTraces().keySet())
      if (t.isAlive() && t.getName().equals(name))
        result.add(t);
    return result;
  }

  private List<String> neighbors() {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT name FROM (SELECT expand(`vector.neighbors`(?, ?, ?)))",
        INDEX_NAME, new float[] { 1.0f, 0.0f, 0.0f }, 1)) {
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
    }
    return names;
  }

  private static IndexInternal bucketLevelVectorIndex(final LocalSchema schema) {
    return ((TypeIndex) schema.getIndexByName(INDEX_NAME)).getIndexesOnBuckets()[0];
  }

  private LocalSchema schema() {
    return ((DatabaseInternal) database).getSchema().getEmbedded();
  }
}
