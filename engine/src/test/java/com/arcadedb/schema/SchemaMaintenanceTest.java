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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.Index;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.IndexWarmUpScheduler.Task;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests, one nested class per fixed issue.
 */
class SchemaMaintenanceTest {

  /**
   * Regression for #10059: a schema change still pending when the database closes (a DDL inside an open
   * transaction postpones its save) was lost as soon as the schema held an index. The close-time save ran once the database
   * was already marked closed, and serializing an index resolves its bucket through the checked schema accessor, which
   * threw {@code DatabaseIsClosedException}: the save was logged and dropped, the close succeeded.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DirtySchemaSavedAtCloseTest extends TestHelper {

    @Test
    void typeAndIndexCreatedInAnOpenTransactionSurviveTheClose() {
      database.getSchema().createDocumentType("Doc").createProperty("name", Type.STRING);
      database.getSchema().getType("Doc").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name");

      database.begin();
      // A DDL INSIDE AN OPEN TRANSACTION POSTPONES ITS SCHEMA SAVE: THE SCHEMA IS DIRTY AT CLOSE
      database.getSchema().createDocumentType("Late").createProperty("k", Type.STRING);
      database.getSchema().getType("Late").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "k");
      assertThat(((LocalSchema) database.getSchema()).isDirty()).isTrue();

      reopenDatabase();

      assertThat(database.getSchema().existsType("Late")).isTrue();
      assertThat(database.getSchema().getType("Late").existsProperty("k")).isTrue();
      final Index index = database.getSchema().getType("Late").getPolymorphicIndexByProperties("k");
      assertThat(index).isNotNull();
      assertThat(((LocalSchema) database.getSchema()).isDirty()).isFalse();

      // THE INDEX IS USABLE ACROSS THE REOPEN, NOT ONLY NAMED BY THE SCHEMA
      database.transaction(() -> database.newDocument("Late").set("k", "v").save());
      assertThat(database.query("sql", "SELECT FROM Late WHERE k = 'v'").stream().count()).isEqualTo(1);
    }

    @Test
    void fullTextIndexCreatedInAnOpenTransactionSurvivesTheClose() {
      database.begin();
      database.getSchema().createDocumentType("Article").createProperty("body", Type.STRING);
      database.getSchema().getType("Article").createTypeIndex(Schema.INDEX_TYPE.FULL_TEXT, false, "body");
      assertThat(((LocalSchema) database.getSchema()).isDirty()).isTrue();

      reopenDatabase();

      assertThat(database.getSchema().existsType("Article")).isTrue();
      assertThat(database.getSchema().getType("Article").getPolymorphicIndexByProperties("body")).isNotNull();
    }
  }

  /**
   * Regression for #10428: the range a delete recorded on a continuous aggregate is cleared by the refresh that recomputed it, and only by
   * that one. A delete recorded after the refresh read the range must survive the clearing even when it lies inside the range and
   * widens nothing: it may have landed after the refresh read the source, so the next refresh has to recompute it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class InvalidationRecordTest extends TestHelper {

    private ContinuousAggregateImpl newAggregate() {
      return new ContinuousAggregateImpl(database, "A", "SELECT 1", "A", "S", 3_600_000L, "hr", "ts");
    }

    @Test
    void aRangeIsMergedIntoTheHullOfTheDeletes() {
      final ContinuousAggregateImpl aggregate = newAggregate();
      assertThat(aggregate.getInvalidation()).isNull();

      aggregate.invalidate(100, 200);
      aggregate.invalidate(150, 400);
      aggregate.invalidate(50, 60);

      assertThat(aggregate.getInvalidation()).isEqualTo(new ContinuousAggregateImpl.Invalidation(50, 400));
    }

    @Test
    void theRefreshThatRecomputedARangeClearsIt() {
      final ContinuousAggregateImpl aggregate = newAggregate();
      aggregate.invalidate(100, 200);

      aggregate.clearInvalidation(aggregate.getInvalidation());

      assertThat(aggregate.getInvalidation()).isNull();
    }

    @Test
    void aDeleteInsideTheRangeRecordedAfterTheRefreshReadItSurvivesTheClearing() {
      final ContinuousAggregateImpl aggregate = newAggregate();
      aggregate.invalidate(100, 400);
      final ContinuousAggregateImpl.Invalidation readByTheRefresh = aggregate.getInvalidation();

      // a delete that lands while the refresh recomputes, inside the range: the hull does not change
      aggregate.invalidate(150, 250);
      assertThat(aggregate.getInvalidation()).isEqualTo(readByTheRefresh);

      aggregate.clearInvalidation(readByTheRefresh);

      assertThat(aggregate.getInvalidation()).as("the delete is recomputed by the next refresh").isNotNull();
    }

    @Test
    void aRefreshAskedForWhileAnotherRunsWithNothingToRecomputeIsSkippedAsItAlwaysWas() {
      final ContinuousAggregateImpl aggregate = newAggregate();
      assertThat(aggregate.tryBeginRefresh()).isTrue();
      try {
        // an append: the refresh in progress or the next append picks its rows up, so the INSERT path asks for nothing more
        ContinuousAggregateRefresher.incrementalRefresh(database, aggregate);

        assertThat(aggregate.isRefreshRequested()).isFalse();
      } finally {
        aggregate.endRefresh();
      }
    }

    @Test
    void aRefreshAskedForWhileAnotherRunsWithARangeToRecomputeIsRemembered() {
      final ContinuousAggregateImpl aggregate = newAggregate();
      aggregate.invalidate(100, 200);
      assertThat(aggregate.tryBeginRefresh()).isTrue();
      try {
        // a delete: the refresh in progress may have read the aggregate before it, so it has to run again when it ends
        ContinuousAggregateRefresher.incrementalRefresh(database, aggregate);

        assertThat(aggregate.isRefreshRequested()).isTrue();
      } finally {
        aggregate.endRefresh();
      }
    }

    @Test
    void onlyARangeThatWidensTheRecordedOneNeedsTheSchemaToBeWritten() {
      final ContinuousAggregateImpl aggregate = newAggregate();

      assertThat(aggregate.invalidate(100, 200)).as("nothing recorded before").isTrue();
      assertThat(aggregate.invalidate(120, 180)).as("inside").isFalse();
      assertThat(aggregate.invalidate(100, 200)).as("the same").isFalse();
      assertThat(aggregate.invalidate(100, 250)).as("later end").isTrue();
      assertThat(aggregate.invalidate(50, 250)).as("earlier start").isTrue();
      assertThat(aggregate.getInvalidation()).isEqualTo(new ContinuousAggregateImpl.Invalidation(50, 250));

      aggregate.clearInvalidation(aggregate.getInvalidation());
      assertThat(aggregate.invalidate(120, 180)).as("nothing recorded any more").isTrue();
    }

    @Test
    void everyRecordedRangeIsANewInstanceEvenWhenItEqualsTheOneThere() {
      final ContinuousAggregateImpl aggregate = newAggregate();
      aggregate.invalidate(100, 200);
      final ContinuousAggregateImpl.Invalidation first = aggregate.getInvalidation();

      aggregate.invalidate(100, 200);
      final ContinuousAggregateImpl.Invalidation second = aggregate.getInvalidation();

      // the clearing of the range a refresh read compares references: it must not take an equal record for the one it read
      assertThat(second).isEqualTo(first).isNotSameAs(first);
    }

    @Test
    void aRangeWhoseClearingCouldNotBePersistedComesBackWithWhatWasRecordedSince() {
      final ContinuousAggregateImpl aggregate = newAggregate();
      aggregate.invalidate(100, 200);
      final ContinuousAggregateImpl.Invalidation readByTheRefresh = aggregate.getInvalidation();
      aggregate.clearInvalidation(readByTheRefresh);
      aggregate.invalidate(500, 600);

      aggregate.restoreInvalidation(readByTheRefresh);

      assertThat(aggregate.getInvalidation()).isEqualTo(new ContinuousAggregateImpl.Invalidation(100, 600));
    }

    @Test
    void theRangeSurvivesTheDefinitionBeingWrittenAndRead() {
      final ContinuousAggregateImpl aggregate = newAggregate();
      aggregate.invalidate(Long.MIN_VALUE, 400);

      final JSONObject json = aggregate.toJSON();
      assertThat(json.getLong("invalidatedFromTs")).isEqualTo(Long.MIN_VALUE);
      assertThat(json.getLong("invalidatedToTs")).isEqualTo(400L);

      assertThat(ContinuousAggregateImpl.fromJSON(database, json).getInvalidation())
          .isEqualTo(new ContinuousAggregateImpl.Invalidation(Long.MIN_VALUE, 400));

      aggregate.clearInvalidation(aggregate.getInvalidation());
      final JSONObject cleared = aggregate.toJSON();
      assertThat(cleared.has("invalidatedFromTs")).isFalse();
      assertThat(ContinuousAggregateImpl.fromJSON(database, cleared).getInvalidation()).isNull();
    }
  }

  /**
   * Regression for #10247: every schema change rebuilt the bucket-to-type maps over every type and assembled the whole schema
   * document before writing it, so a {@code CREATE TYPE} cost O(N) in the number of types and creating N types O(N^2). The
   * maps now follow the additive changes in place, and the schema file is written type by type.
   * <p>
   * The cost is pinned by counting the full rebuilds rather than by timing anything. The maps maintained in place are also
   * compared with a full rebuild at the end of every schema change whenever assertions are enabled, as they are in the
   * tests, so the whole suite checks them; the tests below check them explicitly across every kind of membership change.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SchemaChangeScalingTest extends TestHelper {

    @Test
    void creatingTypesAndPropertiesDoesNotRebuildTheBucketMaps() {
      final LocalSchema schema = localSchema();
      final long rebuildsBefore = schema.getBucketTypeMapRebuilds();

      for (int i = 0; i < 100; i++) {
        schema.createDocumentType("Doc" + i).createProperty("p", Type.STRING);
        schema.createVertexType("Vertex" + i).createProperty("p", Type.STRING);
        schema.createEdgeType("Edge" + i);
      }
      schema.getType("Doc0").createProperty("blob", Type.STRING).setExternal(true);
      database.command("sql", "CREATE BUCKET Extra");
      database.command("sql", "ALTER TYPE Doc1 BUCKET +Extra");

      assertThat(schema.getBucketTypeMapRebuilds()).isEqualTo(rebuildsBefore);
      assertBucketMapsMatchTheTypes(schema);
    }

    @Test
    void theBucketMapsFollowEveryKindOfMembershipChange() {
      final LocalSchema schema = localSchema();
      schema.createDocumentType("A");
      schema.createVertexType("V").createProperty("big", Type.STRING).setExternal(true);
      schema.createEdgeType("E");
      database.command("sql", "CREATE BUCKET Spare");
      database.command("sql", "ALTER TYPE A BUCKET +Spare");
      assertBucketMapsMatchTheTypes(schema);

      // a bucket belongs to one type only
      assertThatThrownBy(() -> database.command("sql", "ALTER TYPE V BUCKET +Spare")).hasMessageContaining("already associated");
      assertThatThrownBy(() -> database.command("sql", "DROP BUCKET Spare")).hasMessageContaining("used by type");

      database.command("sql", "ALTER TYPE A BUCKET -Spare");
      assertThat(schema.getTypeByBucketId(schema.getBucketByName("Spare").getFileId())).isNull();
      assertBucketMapsMatchTheTypes(schema);

      // detached, the bucket can be taken by another type, and dropped once free again
      database.command("sql", "ALTER TYPE V BUCKET +Spare");
      assertThat(schema.getTypeByBucketId(schema.getBucketByName("Spare").getFileId()).getName()).isEqualTo("V");
      database.command("sql", "ALTER TYPE V BUCKET -Spare");
      database.command("sql", "DROP BUCKET Spare");
      assertBucketMapsMatchTheTypes(schema);

      database.command("sql", "ALTER TYPE A NAME Renamed");
      assertBucketMapsMatchTheTypes(schema);

      schema.dropType("E");
      schema.dropType("V");
      assertBucketMapsMatchTheTypes(schema);

      schema.bulkChange(() -> {
        for (int i = 0; i < 20; i++)
          schema.createVertexType("Bulk" + i);
        schema.dropType("Bulk3");
      });
      assertBucketMapsMatchTheTypes(schema);

      reopenDatabase();
      assertBucketMapsMatchTheTypes(localSchema());
      assertThat(localSchema().existsType("Bulk3")).isFalse();
      assertThat(localSchema().existsType("Bulk19")).isTrue();
    }

    @Test
    void theSchemaFileIsTheDocumentTheSchemaDescribes() throws Exception {
      final LocalSchema schema = localSchema();
      final DocumentType doc = schema.createDocumentType("Doc \"quoted\" & <tagged>");
      doc.createProperty("name", Type.STRING).setMandatory(true).setDefaultValue("'x'");
      doc.createProperty("json", Type.MAP);
      doc.setCustomValue("note", "a\nmulti-line\tvalue è");
      schema.createVertexType("Person").createProperty("age", Type.INTEGER);
      database.command("sql", "CREATE INDEX ON Person (age) NOTUNIQUE");
      schema.createEdgeType("Knows");
      database.command("sql", "CREATE TRIGGER t1 BEFORE CREATE ON TYPE Person EXECUTE SQL 'SELECT 1'");

      // a type is reachable under its aliases too, and still written once, where the same key would be repeated
      schema.getType("Knows").setAliases(Set.of("AaaKnows", "ZzzKnows"));
      schema.getType("Person").setAliases(Set.of("Human"));
      schema.createDocumentType("Aaa").setAliases(Set.of("Zzz"));

      assertThat(schema.serializeSchemaForTest()).isEqualTo(schema.toJSON().toString());

      // and what the last change wrote is that document
      final File file = schema.getConfigurationFile();
      try (final FileInputStream in = new FileInputStream(file)) {
        assertThat(FileUtils.readStreamAsString(in, "UTF-8")).isEqualTo(schema.toJSON().toString());
      }
    }

    @Test
    void aSchemaOfManyTypesReopensWithEveryType() {
      final LocalSchema schema = localSchema();
      schema.bulkChange(() -> {
        for (int i = 0; i < 300; i++)
          schema.createDocumentType("T" + i).createProperty("p", Type.STRING);
      });
      reopenDatabase();
      assertThat(localSchema().getTypes()).hasSize(300);
      assertBucketMapsMatchTheTypes(localSchema());
    }

    /** Schema changes from several threads are serialized by the write lock, and the maps they leave describe the types. */
    @Test
    void concurrentSchemaChangesLeaveTheBucketMapsRight() throws Exception {
      final LocalSchema schema = localSchema();
      final int threads = 4;
      final int perThread = 15;
      final CountDownLatch start = new CountDownLatch(1);
      final List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
      final List<Thread> workers = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        final int thread = t;
        final Thread worker = new Thread(() -> {
          try {
            start.await();
            for (int i = 0; i < perThread; i++) {
              final String name = "C" + thread + "_" + i;
              if (i % 3 == 0)
                schema.createVertexType(name).createProperty("p", Type.STRING);
              else
                schema.createDocumentType(name).createProperty("p", Type.STRING);
              if (i % 5 == 0)
                schema.dropType(name);
            }
          } catch (final Throwable e) {
            failures.add(e);
          }
        });
        workers.add(worker);
        worker.start();
      }
      start.countDown();
      for (final Thread worker : workers)
        worker.join();

      assertThat(failures).isEmpty();
      assertThat(schema.getTypes()).hasSize(threads * (perThread - 3));
      assertBucketMapsMatchTheTypes(schema);
    }

    /**
     * A change that fails part way leaves the bucket maps describing the types that are left, not what it had reported
     * before it failed.
     */
    @Test
    void aFailedSchemaChangeLeavesTheBucketMapsRight() {
      final LocalSchema schema = localSchema();
      schema.createDocumentType("Owner");
      final String ownerBucket = schema.getType("Owner").getBuckets(false).getFirst().getName();

      assertThatThrownBy(() -> database.command("sql", "CREATE DOCUMENT TYPE Thief BUCKET " + ownerBucket))//
          .hasMessageContaining("already associated");
      assertThatThrownBy(() -> database.command("sql", "ALTER TYPE Owner BUCKET +" + ownerBucket))//
          .hasMessageContaining("already associated");

      assertThat(schema.getTypeByBucketId(schema.getBucketByName(ownerBucket).getFileId()).getName()).isEqualTo("Owner");
      assertBucketMapsMatchTheTypes(schema);

      schema.createVertexType("Later");
      assertBucketMapsMatchTheTypes(schema);
    }

    /** A change that already attached a bucket when it fails: the first of two buckets is taken, the second is refused. */
    @Test
    void aSchemaChangeThatFailsAfterAttachingABucketLeavesTheBucketMapsRight() {
      final LocalSchema schema = localSchema();
      schema.createDocumentType("Owner");
      final String ownerBucket = schema.getType("Owner").getBuckets(false).getFirst().getName();
      database.command("sql", "CREATE BUCKET Free");

      assertThatThrownBy(() -> database.command("sql", "CREATE DOCUMENT TYPE Partial BUCKET Free," + ownerBucket))//
          .hasMessageContaining("already associated");

      assertThat(schema.getTypeByBucketId(schema.getBucketByName(ownerBucket).getFileId()).getName()).isEqualTo("Owner");
      assertBucketMapsMatchTheTypes(schema);
      // and the next change sees the same maps
      schema.createDocumentType("After");
      assertBucketMapsMatchTheTypes(schema);
    }

    /**
     * The check that a bucket belongs to one type only runs during a load too, against the owners the load has attached so
     * far: a schema file listing one bucket under two types gives it to one of them only.
     */
    @Test
    void aBucketListedUnderTwoTypesInTheSchemaFileGoesToOneOfThemOnLoad() throws Exception {
      final LocalSchema schema = localSchema();
      schema.createDocumentType("First");
      schema.createDocumentType("Second");
      final String firstBucket = schema.getType("First").getBuckets(false).getFirst().getName();
      final File file = schema.getConfigurationFile();
      database.close();

      final String text;
      try (final FileInputStream in = new FileInputStream(file)) {
        text = FileUtils.readStreamAsString(in, "UTF-8");
      }
      final JSONObject json = new JSONObject(text);
      json.getJSONObject("types").getJSONObject("Second").getJSONArray("buckets").put(firstBucket);
      FileUtils.writeContentToStream(file, json.toString().getBytes(StandardCharsets.UTF_8));

      database = factory.open();
      final LocalSchema reloaded = localSchema();
      final int bucketId = reloaded.getBucketByName(firstBucket).getFileId();
      int owners = 0;
      for (final DocumentType type : reloaded.getTypes())
        if (type.getBuckets(false).stream().anyMatch(b -> b.getFileId() == bucketId))
          ++owners;
      assertThat(owners).isLessThanOrEqualTo(1);
    }

    private LocalSchema localSchema() {
      return (LocalSchema) database.getSchema().getEmbedded();
    }

    /** Every bucket a type owns answers to it, and every bucket it involves answers to it unless another type owns it. */
    private static void assertBucketMapsMatchTheTypes(final LocalSchema schema) {
      int owned = 0;
      for (final DocumentType type : schema.getTypes()) {
        for (final Bucket bucket : type.getBuckets(false)) {
          assertThat(schema.getTypeByBucketId(bucket.getFileId())).as(bucket.getName()).isSameAs(type);
          assertThat(schema.getInvolvedTypeByBucketId(bucket.getFileId())).as(bucket.getName()).isSameAs(type);
          ++owned;
        }
        for (final Bucket bucket : type.getInvolvedBuckets()) {
          final DocumentType owner = schema.getTypeByBucketId(bucket.getFileId());
          assertThat(schema.getInvolvedTypeByBucketId(bucket.getFileId())).as(bucket.getName())
              .isSameAs(owner != null ? owner : type);
        }
      }
      // and no bucket answers to a type that does not own it
      int mapped = 0;
      for (final Bucket bucket : schema.getBuckets())
        if (schema.getTypeByBucketId(bucket.getFileId()) != null)
          ++mapped;
      assertThat(mapped).isEqualTo(owned);
    }
  }

  /**
   * Regression for #10021: the indexes a schema reload re-created cold are warmed in the background, one after the other, after the
   * schema has been quiet for a while. Pins the scheduling rules with plain tasks - no clock is asserted on: every
   * ordering below is forced with a latch or with a delay too long to elapse.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class IndexWarmUpSchedulerTest {
    /** Long enough that a request with it can never come due while a test runs. */
    private static final long NEVER = 600_000L;
    /** A bound for waiting on something that must happen, sized as a hang detector and not as a latency claim. */
    private static final long AWAIT_MS = 60_000L;

    private final IndexWarmUpScheduler scheduler = new IndexWarmUpScheduler("issue10021");
    private final List<String>         ran       = new CopyOnWriteArrayList<>();

    @AfterEach
    void tearDown() {
      scheduler.close();
    }

    @Test
    void tasksRunOneAfterTheOtherInRequestOrder() {
      scheduler.request(List.of(task("a"), task("b"), task("c")), 0);

      assertThat(scheduler.awaitIdle(AWAIT_MS)).isTrue();
      assertThat(ran).containsExactly("a", "b", "c");
    }

    @Test
    void aNewerRequestReplacesOneThatHasNotComeDueYet() {
      scheduler.request(List.of(task("old-1"), task("old-2")), NEVER);
      scheduler.request(List.of(task("new")), 0);

      assertThat(scheduler.awaitIdle(AWAIT_MS)).isTrue();
      assertThat(ran).as("the superseded batch holds instances nothing reaches any more").containsExactly("new");
    }

    @Test
    void aNewerRequestAbandonsTheRestOfABatchThatIsRunning() throws Exception {
      final CountDownLatch firstStarted = new CountDownLatch(1);
      final CountDownLatch releaseFirst = new CountDownLatch(1);
      scheduler.request(List.of(new Task("first", () -> {
        firstStarted.countDown();
        await(releaseFirst);
        ran.add("first");
      }), task("second")), 0);
      assertThat(firstStarted.await(AWAIT_MS, TimeUnit.MILLISECONDS)).isTrue();

      scheduler.request(List.of(task("newer")), 0);
      releaseFirst.countDown();

      assertThat(scheduler.awaitIdle(AWAIT_MS)).isTrue();
      assertThat(ran).as("the task in flight finishes; the rest of its batch is dropped").containsExactly("first", "newer");
    }

    @Test
    void aTaskThatFailsDoesNotStopTheOnesAfterIt() {
      scheduler.request(List.of(new Task("broken", () -> {
        throw new IllegalStateException("expected by the test");
      }), task("after")), 0);

      assertThat(scheduler.awaitIdle(AWAIT_MS)).isTrue();
      assertThat(ran).containsExactly("after");
    }

    @Test
    void theNamesOfTheTasksNotYetRunAreReportedSoTheNextReloadCanKeepTheirIntent() throws Exception {
      final CountDownLatch firstStarted = new CountDownLatch(1);
      final CountDownLatch releaseFirst = new CountDownLatch(1);
      scheduler.request(List.of(new Task("running", () -> {
        firstStarted.countDown();
        await(releaseFirst);
      }), task("queued")), 0);
      assertThat(firstStarted.await(AWAIT_MS, TimeUnit.MILLISECONDS)).isTrue();

      assertThat(scheduler.unfinishedNames()).containsExactlyInAnyOrder("running", "queued");

      releaseFirst.countDown();
      assertThat(scheduler.awaitIdle(AWAIT_MS)).isTrue();
      assertThat(scheduler.unfinishedNames()).isEmpty();
    }

    @Test
    void aRequestNotYetDueIsUnfinishedAndIsForgottenWhenReplaced() {
      scheduler.request(List.of(task("waiting")), NEVER);
      assertThat(scheduler.unfinishedNames()).isEqualTo(Set.of("waiting"));

      scheduler.request(List.of(task("replacement")), NEVER);
      assertThat(scheduler.unfinishedNames()).isEqualTo(Set.of("replacement"));
    }

    @Test
    void aClosedSchedulerRunsNothingAndAcceptsNothing() {
      scheduler.request(List.of(task("never")), NEVER);

      scheduler.close();
      scheduler.request(List.of(task("also-never")), 0);

      assertThat(scheduler.awaitIdle(AWAIT_MS)).isTrue();
      assertThat(ran).isEmpty();
      assertThat(scheduler.unfinishedNames()).isEmpty();
    }

    @Test
    void closeInterruptsALoadInFlightAndWaitsForItToEnd() throws Exception {
      final CountDownLatch started = new CountDownLatch(1);
      final CountDownLatch interrupted = new CountDownLatch(1);
      scheduler.request(List.of(new Task("loading", () -> {
        started.countDown();
        try {
          new CountDownLatch(1).await(AWAIT_MS, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
          interrupted.countDown();
          Thread.currentThread().interrupt();
          throw new IllegalStateException("interrupted by the close", e);
        }
      }), task("after")), 0);
      assertThat(started.await(AWAIT_MS, TimeUnit.MILLISECONDS)).isTrue();

      // The worker has ended when close returns, which is what lets the caller close the files it was reading
      assertThat(scheduler.close(AWAIT_MS)).isTrue();

      assertThat(interrupted.getCount()).as("the load was interrupted").isZero();
      assertThat(ran).as("the rest of the batch never ran").isEmpty();
      assertThat(scheduler.awaitIdle(AWAIT_MS)).isTrue();
    }

    @Test
    void closeDoesNotHangOnALoadThatIgnoresTheInterrupt() throws Exception {
      final CountDownLatch started = new CountDownLatch(1);
      final CountDownLatch release = new CountDownLatch(1);
      scheduler.request(List.of(new Task("stubborn", () -> {
        started.countDown();
        // A graph build inside JVector does not honour an interrupt: keep going until released
        boolean released = false;
        while (!released)
          try {
            released = release.await(AWAIT_MS, TimeUnit.MILLISECONDS);
          } catch (final InterruptedException e) {
            // ignored on purpose
          }
      })), 0);
      assertThat(started.await(AWAIT_MS, TimeUnit.MILLISECONDS)).isTrue();

      // Returns, reporting that the worker is still running: the bound, and not the load, decides when close gives up
      assertThat(scheduler.close(1)).isFalse();

      release.countDown();
      assertThat(scheduler.awaitIdle(AWAIT_MS)).isTrue();
    }

    /**
     * Issue #10156: the database close stops the warm-up before its flush, and the schema close calls it again as a backstop. The
     * second call must not charge a second bound for a load that already outlived the first.
     */
    @Test
    void aSecondCloseDoesNotWaitAgainForALoadThatOutlivedTheFirst() throws Exception {
      final CountDownLatch started = new CountDownLatch(1);
      final CountDownLatch release = new CountDownLatch(1);
      scheduler.request(List.of(new Task("stubborn", () -> {
        started.countDown();
        boolean released = false;
        while (!released)
          try {
            released = release.await(AWAIT_MS, TimeUnit.MILLISECONDS);
          } catch (final InterruptedException e) {
            // ignored on purpose, as a JVector graph build does
          }
      })), 0);
      assertThat(started.await(AWAIT_MS, TimeUnit.MILLISECONDS)).isTrue();
      assertThat(scheduler.close(1)).isFalse();

      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      assertThat(scheduler.close(AWAIT_MS)).as("still running, and said so").isFalse();
      stopwatch.assertGaveUpWithin(AWAIT_MS / 2, "a backstop close that returns at once from one that joins the worker again");

      release.countDown();
      assertThat(scheduler.awaitIdle(AWAIT_MS)).isTrue();
      assertThat(scheduler.close(AWAIT_MS)).as("nothing left running").isTrue();
    }

    @Test
    void anEmptyRequestCancelsWhatWasWaiting() {
      scheduler.request(List.of(task("waiting")), NEVER);
      scheduler.request(List.of(), 0);

      assertThat(scheduler.awaitIdle(AWAIT_MS)).isTrue();
      assertThat(ran).isEmpty();
      assertThat(scheduler.unfinishedNames()).isEmpty();
    }

    private Task task(final String name) {
      return new Task(name, () -> ran.add(name));
    }

    private static void await(final CountDownLatch latch) {
      try {
        latch.await(AWAIT_MS, TimeUnit.MILLISECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Regression for #10876: a bucket page smaller than its content header plus one head chunk was accepted by {@code ALTER DATABASE}
   * and by the bucket creation, and every insert into a type created afterwards failed inside the page writer with an
   * offset that named no setting.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class BucketPageSizeFloorTest extends TestHelper {

    private static final String BUCKET_SETTING   = GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getKey();
    private static final String EXTERNAL_SETTING = GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_DEFAULT_PAGE_SIZE.getKey();

    @Test
    void theMinimumIsDerivedFromTheBucketFormatAndStatedBySettings() {
      // page header (8) + record count and slot table (2 + slots * 4) + 4KB of record area (#11286)
      assertThat(LocalBucket.minimumPageSize(LocalBucket.CURRENT_VERSION)).isEqualTo(8 + 2 + 2048 * 4 + 4096);
      assertThat(LocalBucket.minimumPageSize(LocalBucket.EXTERNAL_BUCKET_VERSION)).isEqualTo(8 + 2 + 256 * 4 + 4096);

      assertThat(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getMinimum()).isEqualTo(
          LocalBucket.minimumPageSize(LocalBucket.CURRENT_VERSION));
      assertThat(GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_DEFAULT_PAGE_SIZE.getMinimum()).isEqualTo(
          LocalBucket.minimumPageSize(LocalBucket.EXTERNAL_BUCKET_VERSION));
      // The description is what an operator reads: it must state the minimum the check enforces
      assertThat(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getDescription()).contains(
          "Minimum is " + GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getMinimum() + " bytes");
      assertThat(GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_DEFAULT_PAGE_SIZE.getDescription()).contains(
          "Minimum is " + GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_DEFAULT_PAGE_SIZE.getMinimum() + " bytes");
    }

    @Test
    void alterDatabaseRefusesAPageSizeBelowTheMinimum() {
      final int minimum = LocalBucket.minimumPageSize(LocalBucket.CURRENT_VERSION);
      final Object before = database.getConfiguration().getValue(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE);

      // 8192 is the minimum of an index page, and still below a bucket's
      for (final int tooSmall : new int[] { 4096, 8192, minimum - 1 })
        assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `" + BUCKET_SETTING + "` " + tooSmall).close())
            .hasMessageContaining(BUCKET_SETTING).hasMessageContaining("Minimum is " + minimum);
      assertThat((Object) database.getConfiguration().getValue(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE)).isEqualTo(before);

      assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `" + EXTERNAL_SETTING + "` 1024").close())
          .hasMessageContaining(EXTERNAL_SETTING);
    }

    @Test
    void aTypeCreatedAtTheMinimumPageSizeStoresAndGrowsRecords() {
      final int minimum = LocalBucket.minimumPageSize(LocalBucket.CURRENT_VERSION);
      try (final ResultSet rs = database.command("sql", "ALTER DATABASE `" + BUCKET_SETTING + "` " + minimum)) {
        assertThat(rs.next().<Object>getProperty("newValue")).isEqualTo(minimum);
      }
      database.command("sql", "CREATE DOCUMENT TYPE Smallest").close();
      assertThat(database.getSchema().getType("Smallest").getBuckets(false).getFirst()).isInstanceOf(LocalBucket.class);
      assertThat(((LocalBucket) database.getSchema().getType("Smallest").getBuckets(false).getFirst()).getPageSize()).isEqualTo(minimum);

      final RID[] rids = new RID[2];
      database.transaction(() -> {
        rids[0] = database.newDocument("Smallest").set("v", "x").save().getIdentity();
        rids[1] = database.newDocument("Smallest").set("v", "y".repeat(200)).save().getIdentity();
      });
      database.transaction(() -> {
        final MutableDocument grown = database.lookupByRID(rids[0], true).asDocument().modify();
        grown.set("v", "z".repeat(300)).save();
      });

      assertThat(database.lookupByRID(rids[0], true).asDocument().getString("v")).isEqualTo("z".repeat(300));
      assertThat(database.lookupByRID(rids[1], true).asDocument().getString("v")).isEqualTo("y".repeat(200));
    }

    @Test
    void aBucketBelowTheMinimumIsRefusedWhereverItsPageSizeComesFrom() {
      // Through the Java API the setting is a plain map put, so the creation is the check that always runs
      final Object previous = database.getConfiguration().getValue(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE);
      database.getConfiguration().setValue(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE, 4096);
      try {
        assertThatThrownBy(() -> database.getSchema().createDocumentType("FromTheSetting")).isInstanceOf(SchemaException.class)
            .hasMessageContaining("4096").hasMessageContaining(BUCKET_SETTING);
        // Refused before anything was created: no half-built type, no bucket file
        assertThat(database.getSchema().existsType("FromTheSetting")).isFalse();
        assertThat(database.getSchema().existsBucket("FromTheSetting_0")).isFalse();

        assertThatThrownBy(() -> database.getSchema().createBucket("Loose")).isInstanceOf(SchemaException.class)
            .hasMessageContaining("Loose");
        assertThat(database.getSchema().existsBucket("Loose")).isFalse();
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE, previous);
      }

      assertThatThrownBy(() -> database.command("sql", "CREATE DOCUMENT TYPE FromTheStatement PAGESIZE 4096").close())
          .hasMessageContaining("4096");
      assertThat(database.getSchema().existsType("FromTheStatement")).isFalse();

      // A type growing its bucket count is refused before its first new bucket, and keeps the buckets it had
      database.getSchema().createDocumentType("Growing", 1);
      assertThatThrownBy(() -> database.getSchema().buildDocumentType().withName("Growing").withTotalBuckets(3).withPageSize(4096)
          .withIgnoreIfExists(true).create()).isInstanceOf(SchemaException.class);
      assertThat(database.getSchema().getType("Growing").getBuckets(false)).hasSize(1);
      assertThat(database.getSchema().existsBucket("Growing_1")).isFalse();
    }

    @Test
    void anExternalPropertyBucketBelowItsMinimumIsRefused() {
      database.command("sql", "CREATE DOCUMENT TYPE Heavy").close();
      database.command("sql", "CREATE PROPERTY Heavy.blob STRING").close();
      final Object previous = database.getConfiguration().getValue(GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_DEFAULT_PAGE_SIZE);
      database.getConfiguration().setValue(GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_DEFAULT_PAGE_SIZE, 1024);
      try {
        assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY Heavy.blob EXTERNAL true").close())
            .hasMessageContaining(EXTERNAL_SETTING);
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_DEFAULT_PAGE_SIZE, previous);
      }

      final int minimum = LocalBucket.minimumPageSize(LocalBucket.EXTERNAL_BUCKET_VERSION);
      database.command("sql", "ALTER DATABASE `" + EXTERNAL_SETTING + "` " + minimum).close();
      database.command("sql", "ALTER PROPERTY Heavy.blob EXTERNAL true").close();
      // At the minimum a page holds one byte of a chunk past the head, so a few hundred bytes already span hundreds of chunks
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument("Heavy").set("blob", "b".repeat(300)).save().getIdentity());
      assertThat(database.lookupByRID(rid[0], true).asDocument().getString("blob")).isEqualTo("b".repeat(300));
    }

    @Test
    void aBucketAddedToATypeWithExternalPropertiesIsRefusedWhenItsPairCannotBeCreated() {
      database.command("sql", "CREATE DOCUMENT TYPE Paired BUCKETS 1").close();
      database.command("sql", "CREATE PROPERTY Paired.blob STRING").close();
      database.command("sql", "ALTER PROPERTY Paired.blob EXTERNAL true").close();
      final DocumentType type = database.getSchema().getType("Paired");
      final Bucket loose = database.getSchema().createBucket("Loose");

      final Object previous = database.getConfiguration().getValue(GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_DEFAULT_PAGE_SIZE);
      database.getConfiguration().setValue(GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_DEFAULT_PAGE_SIZE, 1024);
      try {
        // Growing the type: refused before the primary bucket is created, so no bucket is left without its pair
        assertThatThrownBy(() -> database.getSchema().buildDocumentType().withName("Paired").withTotalBuckets(2)
            .withIgnoreIfExists(true).create()).isInstanceOf(SchemaException.class).hasMessageContaining(EXTERNAL_SETTING);
        assertThat(type.getBuckets(false)).hasSize(1);
        assertThat(database.getSchema().existsBucket("Paired_1")).isFalse();

        // Attaching a bucket that exists: refused before it is attached
        assertThatThrownBy(() -> type.addBucket(loose)).isInstanceOf(SchemaException.class).hasMessageContaining(EXTERNAL_SETTING);
        assertThat(type.getBuckets(false)).hasSize(1);
        assertThat(database.getSchema().getTypeByBucketId(loose.getFileId())).isNull();
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_DEFAULT_PAGE_SIZE, previous);
      }

      // With a page size the pair can have, both work and every primary bucket has its pair
      database.getSchema().buildDocumentType().withName("Paired").withTotalBuckets(2).withIgnoreIfExists(true).create();
      type.addBucket(loose);
      assertThat(type.getBuckets(false)).hasSize(3);
      for (final Bucket bucket : type.getBuckets(false))
        assertThat(((LocalDocumentType) type).getExternalBucketIdFor(bucket.getFileId())).isNotNull();
    }
  }
}
