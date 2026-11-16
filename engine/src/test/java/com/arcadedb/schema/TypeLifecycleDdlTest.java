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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.FileManager;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.log.WarningCapture;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Schema regression tests: type lifecycle ddl.
 *
 * <p>One nested class per fixed issue, each keeping its own fixture.</p>
 */
class TypeLifecycleDdlTest {
  /**
   * Regression for #7937: concurrent rename of same type.
   *
   * Regression test for issue #7937: two threads renaming the SAME type to two different names interleaved over the
   * unsynchronised name capture and bucket renames of {@code LocalDocumentType.rename()}. The renames are now
   * serialised by a per-type lock, so the second starts from whatever name the first left: both succeed one after the
   * other and the schema ends up consistent, with the type reachable under exactly one name and its data intact.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ConcurrentRenameOfSameType extends TestHelper {
    private static final int ROUNDS = 25;

    @Test
    void concurrentRenamesOfOneVertexTypeLeaveAConsistentSchema() throws Exception {
      runRounds(true);
    }

    @Test
    void concurrentRenamesOfOneDocumentTypeLeaveAConsistentSchema() throws Exception {
      runRounds(false);
    }

    private void runRounds(final boolean vertex) throws Exception {
      final ExecutorService executor = Executors.newFixedThreadPool(2);
      try {
        for (int round = 0; round < ROUNDS; round++) {
          final String start = "Start" + round;
          final String first = "First" + round;
          final String second = "Second" + round;

          final DocumentType type;
          if (vertex)
            type = database.getSchema().createVertexType(start, 1);
          else
            type = database.getSchema().createDocumentType(start, 1);
          type.createProperty("id", Type.INTEGER);
          type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "id");
          database.transaction(() -> {
            for (int i = 0; i < 10; i++)
              if (vertex)
                database.newVertex(start).set("id", i).save();
              else
                database.newDocument(start).set("id", i).save();
          });

          final CountDownLatch go = new CountDownLatch(1);
          final List<Future<?>> futures = new ArrayList<>();
          for (final String target : new String[] { first, second })
            futures.add(executor.submit(() -> {
              go.await();
              type.rename(target);
              return null;
            }));
          go.countDown();
          for (final Future<?> future : futures)
            future.get(60, TimeUnit.SECONDS);

          // Serialised: the last renamer wins, and the type answers to that single name
          final String finalName = type.getName();
          assertThat(finalName).isIn(first, second);
          assertThat(database.getSchema().existsType(start)).isFalse();
          assertThat(database.getSchema().existsType(first.equals(finalName) ? second : first)).isFalse();
          assertThat(database.getSchema().getType(finalName)).isSameAs(type);

          // Every bucket answers to the name it is registered under, and follows the type's final name
          for (final Bucket bucket : type.getBuckets(false)) {
            assertThat(database.getSchema().existsBucket(bucket.getName())).isTrue();
            assertThat(database.getSchema().getBucketByName(bucket.getName())).isSameAs(bucket);
            assertThat(bucket.getName()).startsWith(finalName + "_");
          }

          // updateTypeName() is part of the racy section: the type's indexes must carry the final name too
          assertThat(type.getAllIndexes(false)).isNotEmpty();
          for (final var index : type.getAllIndexes(false))
            assertThat(index.getName()).startsWith(finalName + "[");

          assertThat(database.countType(finalName, false)).isEqualTo(10L);
        }
      } finally {
        executor.shutdownNow();
      }
    }
  }

  /**
   * Regression for #8064: set aliases atomic reservation.
   *
   * https://github.com/ArcadeData/arcadedb/issues/8064
   * <p>
   * {@code LocalDocumentType.setAliases} mutated the schema's shared type map with a check-then-act sequence and no
   * write lock, which is the arrangement {@code rename()}, {@code createProperty} and {@code dropProperty} were just
   * converted away from. Two holes in the one method:
   * <ol>
   *   <li><b>Check-then-put.</b> It refused an alias already used by another type through
   *   {@code schema.existsType(alias)} and put the alias into {@code schema.typeMap()} several statements later, with
   *   nothing holding the map in between - so two concurrent {@code ALTER TYPE ... ALIASES} on different types could
   *   both pass the check and the second put silently won, leaving two types believing they owned one name.</li>
   *   <li><b>Unconditional deregistration.</b> EVERY previous alias was removed from the map before the new set was
   *   installed, so a reader resolving one of those names saw it disappear even when the new set still carried it.</li>
   * </ol>
   * The class's own comment on the {@code aliases} field already claimed the protection the method never took.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SetAliasesAtomicReservation extends TestHelper {

    private static final int CONCURRENT_ATTEMPTS = 32;

    @Test
    void twoConcurrentSetAliasesOnTheSameNameCannotBothSucceed() throws Exception {
      for (int attempt = 0; attempt < CONCURRENT_ATTEMPTS; attempt++) {
        final String contested = "Shared" + attempt;
        final LocalDocumentType first = (LocalDocumentType) database.getSchema().createDocumentType("A" + attempt);
        final LocalDocumentType second = (LocalDocumentType) database.getSchema().createDocumentType("B" + attempt);

        final CountDownLatch start = new CountDownLatch(1);
        final AtomicInteger succeeded = new AtomicInteger();
        final AtomicReference<Throwable> unexpected = new AtomicReference<>();

        final Thread[] threads = new Thread[2];
        for (int t = 0; t < 2; t++) {
          final LocalDocumentType claimant = t == 0 ? first : second;
          threads[t] = new Thread(() -> {
            try {
              start.await();
              claimant.setAliases(Set.of(contested));
              succeeded.incrementAndGet();
            } catch (final SchemaException e) {
              // THE EXPECTED LOSER'S OUTCOME
            } catch (final Throwable e) {
              unexpected.set(e);
            }
          });
          threads[t].start();
        }

        start.countDown();
        for (final Thread thread : threads) {
          thread.join(TimeUnit.SECONDS.toMillis(30));
          // ASSERTED, NOT ASSUMED: A join() THAT TIMED OUT LEAVES THE CLAIMANT STILL RUNNING, AND EVERY ASSERTION
          // BELOW WOULD THEN BE READING A HALF-FINISHED RESERVATION AND BLAMING IT ON THE FIX
          assertThat(thread.isAlive()).as("claimant thread did not finish on attempt " + attempt).isFalse();
        }

        assertThat(unexpected.get()).as("unexpected failure on attempt " + attempt).isNull();
        assertThat(succeeded.get()).as("exactly one claimant may take '" + contested + "'").isEqualTo(1);

        // AND THE MAP AND THE TWO TYPES AGREE ON WHO WON: EXACTLY ONE OF THEM CARRIES THE ALIAS IN ITS OWN SET, AND
        // THAT IS THE ONE THE NAME RESOLVES TO
        final DocumentType resolved = database.getSchema().getType(contested);
        assertThat(resolved).isNotNull();
        assertThat(first.getAliases().contains(contested) ^ second.getAliases().contains(contested)).isTrue();
        assertThat(resolved.getName()).isEqualTo(first.getAliases().contains(contested) ? first.getName() : second.getName());
      }
    }

    @Test
    void anAliasTheNewSetStillCarriesIsNeverDeregistered() {
      final LocalDocumentType order = (LocalDocumentType) database.getSchema().createDocumentType("Order");
      order.setAliases(Set.of("PO", "PurchaseOrder"));

      // A SUPERSET: BOTH PREVIOUS NAMES SURVIVE, SO NEITHER MAY EVER LEAVE THE MAP
      order.setAliases(Set.of("PO", "PurchaseOrder", "Ordine"));

      assertThat(database.getSchema().getType("PO").getName()).isEqualTo("Order");
      assertThat(database.getSchema().getType("PurchaseOrder").getName()).isEqualTo("Order");
      assertThat(database.getSchema().getType("Ordine").getName()).isEqualTo("Order");
      assertThat(order.getAliases()).containsExactlyInAnyOrder("PO", "PurchaseOrder", "Ordine");
    }

    @Test
    void anAliasTheNewSetDropsStopsResolving() {
      final LocalDocumentType order = (LocalDocumentType) database.getSchema().createDocumentType("Order");
      order.setAliases(Set.of("PO", "PurchaseOrder"));

      order.setAliases(Set.of("PO"));

      assertThat(database.getSchema().existsType("PurchaseOrder")).isFalse();
      assertThat(database.getSchema().getType("PO").getName()).isEqualTo("Order");
      assertThat(order.getAliases()).containsExactly("PO");

      order.setAliases(Set.of());
      assertThat(database.getSchema().existsType("PO")).isFalse();
      assertThat(order.getAliases()).isEmpty();
    }

    @Test
    void aRefusedCallLeavesNoHalfInstalledAliasBehind() {
      final LocalDocumentType order = (LocalDocumentType) database.getSchema().createDocumentType("Order");
      database.getSchema().createDocumentType("Invoice");

      // ONE FREE NAME AND ONE ALREADY TAKEN. WHICHEVER ORDER THE SET ITERATES IN, THE FREE ONE MUST NOT SURVIVE A
      // CALL THAT WAS REFUSED - THE ALIASES ARE ALL-OR-NOTHING
      assertThatThrownBy(() -> order.setAliases(new HashSet<>(Set.of("PO", "Invoice")))).isInstanceOf(SchemaException.class)
          .hasMessageContaining("Invoice");

      assertThat(order.getAliases()).isEmpty();
      assertThat(database.getSchema().existsType("PO")).isFalse();
      assertThat(database.getSchema().getType("Invoice").getName()).isEqualTo("Invoice");
    }

    @Test
    void anAliasEqualToTheTypesOwnNameIsRefusedWithoutBlamingItself() {
      // The type map holds the type's own name too, so putIfAbsent hands back THIS type - and the generic wording
      // would name it on both sides of the sentence, which reads as an engine bug rather than as a refusal.
      final LocalDocumentType order = (LocalDocumentType) database.getSchema().createDocumentType("Order");

      assertThatThrownBy(() -> order.setAliases(Set.of("Order"))).isInstanceOf(SchemaException.class)
          .hasMessageContaining("is the name of the type itself")
          .hasMessageNotContaining("already used by type");

      assertThat(order.getAliases()).isEmpty();
      assertThat(database.getSchema().getType("Order").getName()).isEqualTo("Order");
    }

    @Test
    void aRefusedCallKeepsThePreviousAliasesIntact() {
      final LocalDocumentType order = (LocalDocumentType) database.getSchema().createDocumentType("Order");
      order.setAliases(Set.of("PO"));
      database.getSchema().createDocumentType("Invoice");

      assertThatThrownBy(() -> order.setAliases(new HashSet<>(Set.of("PO", "Invoice")))).isInstanceOf(SchemaException.class);

      assertThat(order.getAliases()).containsExactly("PO");
      assertThat(database.getSchema().getType("PO").getName()).isEqualTo("Order");
    }

    @Test
    void aliasesSurviveAReopen() {
      final LocalDocumentType order = (LocalDocumentType) database.getSchema().createDocumentType("Order");
      order.setAliases(Set.of("PO", "PurchaseOrder"));

      // recordFileChanges() SAVES schema.json ITSELF, WHICH IS WHY THE EXPLICIT saveConfiguration() CALL WENT AWAY -
      // THIS IS WHAT PROVES THE SAVE STILL HAPPENS
      reopenDatabase();

      assertThat(database.getSchema().getType("PO").getName()).isEqualTo("Order");
      assertThat(database.getSchema().getType("PurchaseOrder").getName()).isEqualTo("Order");
    }
  }

  /**
   * Regression for #8169: drop type leaves aliases.
   *
   * https://github.com/ArcadeData/arcadedb/issues/8169
   * <p>
   * {@code LocalSchema.dropType} removed exactly one key from the schema's shared type map - the type's own name - and
   * nothing anywhere removed the alias keys, which kept pointing at the dropped, bucket-less {@code LocalDocumentType}
   * instance. Three consequences, in increasing order of damage:
   * <ol>
   *   <li>{@code existsType(alias)} kept answering {@code true} and {@code getType(alias)} kept handing out the
   *   dropped type.</li>
   *   <li>A later {@code ALTER TYPE Other ALIASES <alias>} was refused naming a type that no longer existed - #8064's
   *   atomic {@code putIfAbsent} turned the stale entry from a silent overwrite into a hard refusal.</li>
   *   <li>{@code toJSON()} - reached from {@code saveConfiguration()} - iterates {@code typeMap().values()} and
   *   keys each entry by {@code t.getName()}, so the dropped instance, still reachable through its surviving alias
   *   key, was written back into {@code schema.json} under its own name with zero buckets and came back alive at
   *   the next open.</li>
   * </ol>
   * The drop also resolves its argument through {@code getType()}, which resolves aliases, so {@code DROP TYPE <alias>}
   * tore down the real type's buckets and indexes and then removed only the alias key, leaving the type itself
   * registered under its own name and unusable.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DropTypeLeavesAliases extends TestHelper {

    @Test
    void dropTypeDeregistersTheTypeAliases() {
      final DocumentType order = database.getSchema().createDocumentType("Order");
      database.getSchema().createDocumentType("Invoice");
      order.setAliases(Set.of("PO", "PurchaseOrder"));

      assertThat(database.getSchema().existsType("PO")).isTrue();
      assertThat(database.getSchema().existsType("PurchaseOrder")).isTrue();

      database.getSchema().dropType("Order");

      assertThat(database.getSchema().existsType("Order")).isFalse();
      assertThat(database.getSchema().existsType("PO")).isFalse();
      assertThat(database.getSchema().existsType("PurchaseOrder")).isFalse();
      assertThat(database.getSchema().getTypeOrNull("PO")).isNull();
      assertThatThrownBy(() -> database.getSchema().getType("PO")).isInstanceOf(SchemaException.class);

      // THE DROPPED INSTANCE IS NO LONGER LISTED: getTypes() deduplicates typeMap().values(), but a surviving alias
      // key kept the instance IN those values
      assertThat(database.getSchema().getTypes()).extracting(DocumentType::getName).containsExactly("Invoice");
    }

    @Test
    void anAliasFreedByDropTypeCanBeClaimedByAnotherType() {
      final DocumentType order = database.getSchema().createDocumentType("Order");
      final DocumentType invoice = database.getSchema().createDocumentType("Invoice");
      order.setAliases(Set.of("PO"));

      database.getSchema().dropType("Order");

      // BEFORE THE FIX THIS WAS REFUSED WITH "already used by type 'Order'", NAMING A TYPE THAT NO LONGER EXISTED
      invoice.setAliases(Set.of("PO"));

      assertThat(database.getSchema().existsType("PO")).isTrue();
      assertThat(database.getSchema().getType("PO")).isSameAs(invoice);
    }

    @Test
    void sqlDropTypeDeregistersTheTypeAliasesAndFreesThemForAlterType() {
      database.command("sql", "CREATE DOCUMENT TYPE Order");
      database.command("sql", "CREATE DOCUMENT TYPE Invoice");
      database.command("sql", "ALTER TYPE Order ALIASES PO");

      assertThat(database.getSchema().existsType("PO")).isTrue();

      database.command("sql", "DROP TYPE Order");

      assertThat(database.getSchema().existsType("Order")).isFalse();
      assertThat(database.getSchema().existsType("PO")).isFalse();

      database.command("sql", "ALTER TYPE Invoice ALIASES PO");

      assertThat(database.getSchema().getType("PO").getName()).isEqualTo("Invoice");
    }

    @Test
    void aDroppedTypeDoesNotComeBackAfterAReopen() throws IOException {
      final DocumentType order = database.getSchema().createDocumentType("Order");
      database.getSchema().createDocumentType("Invoice");
      order.setAliases(Set.of("PO"));

      database.getSchema().dropType("Order");

      // THE FILE ITSELF, not only what the reopen makes of it: toJSON() keys typeMap().values() by t.getName(), and
      // the dropped instance was still in those values through its surviving alias key, so it was written back under
      // its own name with an empty bucket list
      final JSONObject onDisk = new JSONObject(
          Files.readString(Path.of(getDatabasePath(), "schema.json"), StandardCharsets.UTF_8));
      assertThat(onDisk.getJSONObject("types").keySet()).containsExactly("Invoice");

      reopenDatabase();

      // THE ONE THAT MATTERS MOST: the dropped instance used to be re-serialised into schema.json under its own name
      // with zero buckets, so it was back - permanently - at the next open, and unusable
      assertThat(database.getSchema().existsType("Order")).isFalse();
      assertThat(database.getSchema().existsType("PO")).isFalse();
      assertThat(database.getSchema().getTypes()).extracting(DocumentType::getName).containsExactly("Invoice");
    }

    @Test
    void dropTypeThroughAnAliasDropsTheTypeItself() {
      final DocumentType order = database.getSchema().createDocumentType("Order");
      database.getSchema().createDocumentType("Invoice");
      order.setAliases(Set.of("PO"));

      // getType() resolves aliases, so the drop tears down the real type. It has to deregister it under every name it
      // answers to, not only under the alias the caller typed
      database.getSchema().dropType("PO");

      assertThat(database.getSchema().existsType("PO")).isFalse();
      assertThat(database.getSchema().existsType("Order")).isFalse();

      reopenDatabase();

      assertThat(database.getSchema().existsType("Order")).isFalse();
      assertThat(database.getSchema().existsType("PO")).isFalse();
    }

    @Test
    void dropTypeThroughAnAliasIsStillRefusedForAMaterializedViewSourceType() {
      database.transaction(() -> database.getSchema().createDocumentType("Employee"));
      database.transaction(() -> database.getSchema().getType("Employee").setAliases(Set.of("Emp")));
      database.transaction(() -> database.newDocument("Employee").set("name", "Alice").save());
      database.transaction(() -> database.getSchema().buildMaterializedView()
          .withName("EmployeeView")
          .withQuery("SELECT name FROM Employee")
          .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL)
          .create());

      // The guard used to compare the caller's string against the view's source type names, so naming the type by one
      // of its aliases walked straight past it and dropped the view's source type out from under it
      assertThatThrownBy(() -> database.getSchema().dropType("Emp"))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("EmployeeView");

      assertThat(database.getSchema().existsType("Employee")).isTrue();
      assertThat(database.getSchema().existsType("Emp")).isTrue();
    }

    @Test
    void dropTypeThroughAnAliasIsStillRefusedForAContinuousAggregateSourceType() {
      database.command("sql",
          "CREATE TIMESERIES TYPE SensorReading TIMESTAMP ts TAGS (sensor_id STRING) FIELDS (temperature DOUBLE)");
      database.getSchema().getType("SensorReading").setAliases(Set.of("Sensor"));
      database.transaction(() -> database.command("sql",
          "INSERT INTO SensorReading SET ts = 1000, sensor_id = 'A', temperature = 22.5"));
      database.getSchema().buildContinuousAggregate()
          .withName("hourly_temps")
          .withQuery("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp "
              + "FROM SensorReading GROUP BY sensor_id, hour")
          .create();

      // The continuous-aggregate guard is the twin of the materialized-view one above and was bypassed the same way
      assertThatThrownBy(() -> database.getSchema().dropType("Sensor"))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("hourly_temps");

      assertThat(database.getSchema().existsType("SensorReading")).isTrue();
      assertThat(database.getSchema().existsType("Sensor")).isTrue();
    }

    @Test
    void dropTypeThroughAnAliasIsStillRefusedForAContinuousAggregateBackingType() {
      database.command("sql",
          "CREATE TIMESERIES TYPE SensorReading TIMESTAMP ts TAGS (sensor_id STRING) FIELDS (temperature DOUBLE)");
      database.transaction(() -> database.command("sql",
          "INSERT INTO SensorReading SET ts = 1000, sensor_id = 'A', temperature = 22.5"));
      final String backingTypeName = database.getSchema().buildContinuousAggregate()
          .withName("hourly_temps")
          .withQuery("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp "
              + "FROM SensorReading GROUP BY sensor_id, hour")
          .create()
          .getBackingType()
          .getName();

      database.getSchema().getType(backingTypeName).setAliases(Set.of("HourlyTemps"));

      assertThatThrownBy(() -> database.getSchema().dropType("HourlyTemps"))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("hourly_temps");

      assertThat(database.getSchema().existsType(backingTypeName)).isTrue();
      assertThat(database.getSchema().existsType("HourlyTemps")).isTrue();
    }
  }

  /**
   * Regression for #8187: bucketless zombie type.
   *
   * https://github.com/ArcadeData/arcadedb/issues/8187
   * <p>
   * A {@code schema.json} written by a version affected by #8169 carries a dropped type as a real entry with an empty
   * bucket list. It was loaded silently and the only symptom was the first insert failing. The open now names every
   * bucket-less type that can never hold a record (no buckets and no subtypes) and says how to remove it, the insert
   * error says the same, and {@code DROP TYPE} removes the zombie for good.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class BucketlessZombieType extends TestHelper {

    @Test
    void aZombieTypeIsReportedAtOpenAndDropTypeRemovesItForGood() throws IOException {
      database.getSchema().createDocumentType("Invoice");
      injectZombie("Order", "PO");

      final List<String> warnings = WarningCapture.captureWarnings(this::reopenDatabase);
      assertThat(warnings).anyMatch(w -> w.contains("'Order'") && w.contains("DROP TYPE"));
      assertThat(warnings).noneMatch(w -> w.contains("'Invoice'") && w.contains("no buckets"));

      assertThat(database.getSchema().existsType("Order")).isTrue();
      assertThat(database.getSchema().existsType("PO")).isTrue();

      // UNDER HA loadIncremental() RUNS ONCE PER REPLICATED DDL: THE ZOMBIE IS REPORTED ONCE, NOT AFTER EVERY STATEMENT
      final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();
      final List<String> incremental = WarningCapture.captureWarnings(() -> {
        try {
          schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of());
        } catch (final IOException e) {
          throw new UncheckedIOException(e);
        }
      });
      assertThat(incremental).noneMatch(w -> w.contains("'Order'"));

      assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Order").set("a", 1).save()))
          .isInstanceOf(SchemaException.class).hasMessageContaining("Order").hasMessageContaining("DROP TYPE");

      database.command("sql", "DROP TYPE Order");
      assertThat(database.getSchema().existsType("Order")).isFalse();
      assertThat(database.getSchema().existsType("PO")).isFalse();

      final List<String> afterDrop = WarningCapture.captureWarnings(this::reopenDatabase);
      assertThat(afterDrop).noneMatch(w -> w.contains("'Order'"));
      assertThat(database.getSchema().existsType("Order")).isFalse();
      assertThat(database.getSchema().existsType("PO")).isFalse();
      assertThat(database.getSchema().getTypes()).extracting(DocumentType::getName).containsExactly("Invoice");
    }

    @Test
    void aBucketlessSuperTypeIsNotReported() {
      final DocumentType base = database.getSchema().createDocumentType("Base");
      database.getSchema().createDocumentType("Child").addSuperType(base);
      for (final var bucket : List.copyOf(base.getBuckets(false)))
        base.removeBucket(bucket);
      assertThat(base.getBuckets(false)).isEmpty();

      final List<String> warnings = WarningCapture.captureWarnings(this::reopenDatabase);
      assertThat(warnings).noneMatch(w -> w.contains("'Base'"));
      assertThat(database.getSchema().getType("Base").getBuckets(false)).isEmpty();
    }

    @Test
    void aTypeRecreatedUnderADroppedZombieNameIsReportedAgain() throws IOException {
      database.getSchema().createDocumentType("Invoice");
      injectZombie("Order", "PO");
      assertThat(WarningCapture.captureWarnings(this::reopenDatabase)).anyMatch(w -> w.contains("'Order'"));

      database.command("sql", "DROP TYPE Order");

      // A NEW, UNRELATED TYPE UNDER THE SAME NAME THAT ENDS UP BUCKET-LESS TOO MUST NOT INHERIT THE "ALREADY REPORTED"
      final DocumentType order = database.getSchema().createDocumentType("Order");
      for (final var bucket : List.copyOf(order.getBuckets(false)))
        order.removeBucket(bucket);

      final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();
      assertThat(WarningCapture.captureWarnings(() -> {
        try {
          schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of());
        } catch (final IOException e) {
          throw new UncheckedIOException(e);
        }
      })).anyMatch(w -> w.contains("'Order'"));
    }

    private void injectZombie(final String name, final String alias) throws IOException {
      database.close();

      final Path schemaFile = Path.of(getDatabasePath(), "schema.json");
      final JSONObject root = new JSONObject(Files.readString(schemaFile, StandardCharsets.UTF_8));
      root.getJSONObject("types").put(name, new JSONObject()//
          .put("type", "d").put("parents", new JSONArray()).put("buckets", new JSONArray())
          .put("aliases", new JSONArray().put(alias)).put("properties", new JSONObject()).put("indexes", new JSONObject())
          .put("custom", new JSONObject()));
      Files.writeString(schemaFile, root.toString(), StandardCharsets.UTF_8);
      database = null;
    }
  }

  /**
   * Regression for #9762: type rename records file renames.
   *
   * Issue #9762: a type rename moves its files before it opens the schema recording session that publishes it, so it hands
   * that session the renames itself. Under HA the session ships them to the followers, which move the same files; a file
   * left out is a file a follower keeps under the old name while its schema names the new one.
   * <p>
   * The session is opened here by hand, the way the HA layer opens it around the publishing frame, and what the rename
   * recorded is compared with the files that actually moved on disk.
   */
  @Nested
  class TypeRenameRecordsFileRenames extends TestHelper {

    @Test
    void aDocumentTypeRenameRecordsEveryBucketAndIndexFileItMoved() {
      database.command("sql", "CREATE DOCUMENT TYPE R BUCKETS 3");
      database.command("sql", "CREATE PROPERTY R.id INTEGER");
      database.command("sql", "CREATE INDEX ON R (id) UNIQUE");
      database.transaction(() -> {
        for (int i = 0; i < 20; i++)
          database.newDocument("R").set("id", i).save();
      });

      final Map<Integer, String> before = componentNames();
      final Map<Integer, String> recorded = renameRecording("R", "R2");

      assertThat(recorded).as("the 3 buckets and the 3 sub-index files").hasSize(6);
      assertRecordedIsWhatMoved(before, recorded, "R2");
    }

    @Test
    void aVertexTypeRenameRecordsItsEdgeBucketsToo() {
      database.command("sql", "CREATE VERTEX TYPE Vx BUCKETS 2");
      database.command("sql", "CREATE EDGE TYPE Ex");
      database.transaction(() -> {
        MutableVertex previous = null;
        for (int i = 0; i < 10; i++) {
          final MutableVertex v = database.newVertex("Vx").save();
          if (previous != null)
            previous.newEdge("Ex", v).save();
          previous = v;
        }
      });

      final Map<Integer, String> before = componentNames();
      final List<Bucket> involved = database.getSchema().getType("Vx").getInvolvedBuckets();
      final Map<Integer, String> recorded = renameRecording("Vx", "Vx2");

      assertThat(involved.size()).as("buckets and their edge buckets").isGreaterThan(2);
      for (final Bucket bucket : involved)
        assertThat(recorded).as("bucket %s", bucket.getName()).containsKey(bucket.getFileId());
      assertRecordedIsWhatMoved(before, recorded, "Vx2");
    }

    @Test
    void anotherTypesFilesAreNeverRecorded() {
      database.command("sql", "CREATE DOCUMENT TYPE A");
      database.command("sql", "CREATE DOCUMENT TYPE B");
      final int bucketOfB = database.getSchema().getType("B").getBuckets(false).getFirst().getFileId();

      final Map<Integer, String> recorded = renameRecording("A", "A2");

      assertThat(recorded).isNotEmpty().doesNotContainKey(bucketOfB);
    }

    @Test
    void withoutASessionNothingIsRecordedAndTheRenameStillHappens() {
      database.command("sql", "CREATE DOCUMENT TYPE N");

      database.getSchema().getType("N").rename("N2");

      assertThat(database.getSchema().existsType("N2")).isTrue();
      assertThat(fileManager().getRecordedRenames()).isEmpty();
    }

    /**
     * The publishing session refuses after the files were moved - under HA, leadership lost between the pre-check and the
     * session. The wrapper refuses the session the way {@code RaftReplicatedDatabase} would, through the same seam the HA
     * server installs it on. The rename is put back: type, buckets and index files under the old name, after a reopen too.
     */
    @Test
    void aRefusalFromThePublishingSessionPutsEverythingBack() {
      database.command("sql", "CREATE DOCUMENT TYPE P BUCKETS 2");
      database.command("sql", "CREATE PROPERTY P.id INTEGER");
      database.command("sql", "CREATE INDEX ON P (id) UNIQUE");
      database.transaction(() -> {
        for (int i = 0; i < 10; i++)
          database.newDocument("P").set("id", i).save();
      });
      final Map<Integer, String> before = componentNames();

      final LocalDatabase local = (LocalDatabase) database;
      final DatabaseInternal previous = local.getWrappedDatabaseInstance();
      final boolean[] refused = new boolean[1];
      local.setWrappedDatabaseInstance((DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
          new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
            if ("recordFileChanges".equals(method.getName())) {
              refused[0] = true;
              throw new NeedRetryException("leadership lost (issue 9762 test)");
            }
            try {
              return method.invoke(local, args);
            } catch (final InvocationTargetException e) {
              throw e.getCause();
            }
          }));
      try {
        assertThatThrownBy(() -> database.getSchema().getType("P").rename("P2")).isInstanceOf(NeedRetryException.class)
            .hasMessageContaining("leadership lost");
      } finally {
        local.setWrappedDatabaseInstance(previous);
      }

      assertThat(refused[0]).as("the refusal came from the publishing session, after the files moved").isTrue();
      assertThat(componentNames()).as("every file back under its old name").isEqualTo(before);
      assertThat(database.getSchema().existsType("P")).isTrue();
      assertThat(database.getSchema().existsType("P2")).isFalse();

      reopenDatabase();

      assertThat(database.getSchema().existsType("P")).isTrue();
      assertThat(database.getSchema().existsType("P2")).isFalse();
      assertThat(database.countType("P", false)).isEqualTo(10);
      assertThat(database.query("sql", "SELECT FROM P WHERE id = 7").hasNext()).isTrue();
    }

    /** Renames {@code from} inside a recording session and returns the renames it recorded. */
    private Map<Integer, String> renameRecording(final String from, final String to) {
      final FileManager fileManager = fileManager();
      assertThat(fileManager.startRecordingChanges()).isTrue();
      try {
        database.getSchema().getType(from).rename(to);
        return fileManager.getRecordedRenames();
      } finally {
        fileManager.stopRecordingChanges();
      }
    }

    /** Every recorded rename names the file's current component, and every file that changed name was recorded. */
    private void assertRecordedIsWhatMoved(final Map<Integer, String> before, final Map<Integer, String> recorded,
        final String newTypeName) {
      final Map<Integer, String> after = componentNames();
      for (final Map.Entry<Integer, String> rename : recorded.entrySet()) {
        assertThat(after.get(rename.getKey())).as("file %d", rename.getKey()).isEqualTo(rename.getValue());
        assertThat(rename.getValue()).startsWith(newTypeName + "_");
      }
      for (final Map.Entry<Integer, String> file : after.entrySet())
        if (before.containsKey(file.getKey()) && !before.get(file.getKey()).equals(file.getValue()))
          assertThat(recorded).as("moved file %d '%s' -> '%s'", file.getKey(), before.get(file.getKey()), file.getValue())
              .containsEntry(file.getKey(), file.getValue());

      final DocumentType type = database.getSchema().getType(newTypeName);
      for (final TypeIndex index : type.getAllIndexes(false))
        for (final IndexInternal sub : index.getIndexesOnBuckets())
          assertThat(recorded).as("index file of %s", sub.getName()).containsKey(sub.getFileId());
    }

    private Map<Integer, String> componentNames() {
      final Map<Integer, String> names = new HashMap<>();
      for (final ComponentFile file : fileManager().getFiles())
        if (file != null && file.getComponentName() != null)
          names.put(file.getFileId(), file.getComponentName());
      return names;
    }

    private FileManager fileManager() {
      return ((DatabaseInternal) database).getFileManager();
    }
  }

  /**
   * Regression for #9851: copy type keeps the caller transaction.
   *
   * Regression test for issue #9851: {@code LocalSchema.copyType()} copied the records in a transaction of its own and closed it with
   * {@code if (db.isTransactionActive()) db.rollback()}. An ArcadeDB nested transaction is an independent transaction rather than a
   * savepoint, and its {@code commit()} pops the context, so on a thread that already had a transaction open the question was answered
   * for the CALLER's transaction and the rollback discarded the caller's uncommitted work - on the successful path as well as the
   * failing one. The method now holds an {@code OwnTransaction} and rolls back only that.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CopyTypeKeepsTheCallerTransaction extends TestHelper {

    private static final int RECORDS = 25;

    private DatabaseInternal internal() {
      return (DatabaseInternal) database;
    }

    private void createSource() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Source").createProperty("n", Type.INTEGER);
        for (int i = 0; i < RECORDS; i++)
          database.newDocument("Source").set("n", i).save();
      });
    }

    /** The caller's own pending write survives a copy that succeeds, and is still its to commit. */
    @Test
    void copyTypeSucceedingInsideACallerTransactionKeepsTheCallersPendingWork() {
      createSource();
      database.getSchema().createDocumentType("Witness");

      database.begin();
      try {
        database.newDocument("Witness").set("w", "pending").save();
        final TransactionContext callers = internal().getTransaction();

        database.getSchema().copyType("Source", "Target", LocalDocumentType.class, 1, 65_536, 10);

        assertThat(database.isTransactionActive()).as("the caller's transaction must still be open").isTrue();
        assertThat(internal().getTransaction()).as("and be the same one").isSameAs(callers);
        assertThat(database.countType("Witness", false)).as("the caller's pending write is still visible to it").isEqualTo(1L);

        database.commit();
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }

      assertThat(database.countType("Witness", false)).as("the caller's work reached the database").isEqualTo(1L);
      assertThat(database.countType("Target", false)).as("and so did the copy").isEqualTo((long) RECORDS);
    }

    /** A single batch (batch size 0 commits everything once, at the end) is the same case with one commit instead of several. */
    @Test
    void copyTypeInOneBatchInsideACallerTransactionKeepsTheCallersPendingWork() {
      createSource();
      database.getSchema().createDocumentType("Witness");

      database.begin();
      try {
        database.newDocument("Witness").set("w", "pending").save();

        database.getSchema().copyType("Source", "Target", LocalDocumentType.class, 1, 65_536, 0);

        assertThat(database.isTransactionActive()).isTrue();
        assertThat(database.countType("Witness", false)).isEqualTo(1L);
        database.commit();
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }

      assertThat(database.countType("Witness", false)).isEqualTo(1L);
      assertThat(database.countType("Target", false)).isEqualTo((long) RECORDS);
    }

    /**
     * The failing arm: a record that cannot be created makes the copy throw mid-way, after one batch already committed. The failure
     * rolls back the batch in flight and nothing else: the copy is dropped by the method's own safety net, and the caller's
     * transaction, with its pending write, is untouched.
     */
    @Test
    void copyTypeFailingInsideACallerTransactionKeepsTheCallersPendingWork() {
      createSource();
      database.getSchema().createDocumentType("Witness");

      final AtomicInteger created = new AtomicInteger();
      final BeforeRecordCreateListener failOnTheFifteenthCopiedRecord = record -> {
        if ("Target".equals(((Document) record).getTypeName()) && created.incrementAndGet() == 15)
          throw new IllegalStateException("simulated failure while copying");
        return true;
      };
      database.getEvents().registerListener(failOnTheFifteenthCopiedRecord);
      try {
        database.begin();
        try {
          database.newDocument("Witness").set("w", "pending").save();
          final TransactionContext callers = internal().getTransaction();

          assertThatThrownBy(() -> database.getSchema().copyType("Source", "Target", LocalDocumentType.class, 1, 65_536, 10))
              .isInstanceOf(IllegalStateException.class);

          assertThat(database.isTransactionActive()).as("the caller's transaction must still be open").isTrue();
          assertThat(internal().getTransaction()).as("and be the same one").isSameAs(callers);
          assertThat(database.countType("Witness", false)).as("the caller's pending write is still visible to it").isEqualTo(1L);

          database.commit();
        } finally {
          if (database.isTransactionActive())
            database.rollback();
        }
      } finally {
        database.getEvents().unregisterListener(failOnTheFifteenthCopiedRecord);
      }

      assertThat(database.countType("Witness", false)).as("the caller's work reached the database").isEqualTo(1L);
      assertThat(database.getSchema().existsType("Target")).as("the half-built copy is dropped").isFalse();
      assertThat(database.countType("Source", false)).as("the source is untouched").isEqualTo((long) RECORDS);
    }

    /** The schema must not be asked to leave a half-built copy behind, and the engine must not leak a transaction of its own. */
    @Test
    void copyTypeLeavesNoTransactionOfItsOwnOnTheThread() {
      createSource();

      database.getSchema().copyType("Source", "Target", LocalDocumentType.class, 1, 65_536, 10);

      assertThat(database.isTransactionActive()).as("no transaction is left open on a thread that had none").isFalse();
      assertThat(database.countType("Target", false)).isEqualTo((long) RECORDS);
    }
  }

  /**
   * Regression for #9891: type rename publish signal.
   *
   * Issue #9891 (item 2): which call of a type rename publishes is said by the entry point, not read off the hold count of
   * the type's rename lock. {@code rename()} always publishes, once; {@code renameDocumentPart()} is the nested step a
   * subclass uses and never publishes. A caller that took the lock and then called {@code rename()} used to look nested and
   * its rename was never published, silently; it is now refused before anything moves.
   */
  @Nested
  class TypeRenamePublishSignal extends TestHelper {

    @Test
    void aRenameCalledWhileTheThreadAlreadyHoldsTheRenameLockIsRefusedBeforeAnythingMoves() {
      database.command("sql", "CREATE DOCUMENT TYPE Held BUCKETS 2");
      database.transaction(() -> database.newDocument("Held").set("id", 1).save());
      final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType("Held");
      final String bucketName = type.getBuckets(false).getFirst().getName();

      final AtomicInteger publishes = countPublishes(() -> {
        type.renameLock.lock();
        try {
          assertThatThrownBy(() -> type.rename("Held2")).isInstanceOf(IllegalStateException.class)
              .hasMessageContaining("already being renamed");
        } finally {
          type.renameLock.unlock();
        }
      });

      assertThat(publishes).hasValue(0);
      assertThat(database.getSchema().existsType("Held")).isTrue();
      assertThat(database.getSchema().existsType("Held2")).isFalse();
      assertThat(type.getBuckets(false).getFirst().getName()).as("no file moved").isEqualTo(bucketName);
    }

    @Test
    void theDocumentPartCannotBeRenamedOutsideARename() {
      database.command("sql", "CREATE DOCUMENT TYPE Outside");
      final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType("Outside");

      assertThatThrownBy(() -> type.renameDocumentPart("Outside2")).isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("call rename()");

      assertThat(database.getSchema().existsType("Outside")).isTrue();
      assertThat(database.getSchema().existsType("Outside2")).isFalse();
    }

    /** A vertex type renames its document part nested inside its own rename: still exactly one publication. */
    @Test
    void aVertexTypeRenamePublishesExactlyOnce() {
      database.command("sql", "CREATE VERTEX TYPE Pv BUCKETS 2");
      database.command("sql", "CREATE EDGE TYPE Pe");
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("Pv").save();
        final MutableVertex b = database.newVertex("Pv").save();
        a.newEdge("Pe", b).save();
      });

      final AtomicInteger publishes = countPublishes(() -> database.getSchema().getType("Pv").rename("Pv2"));

      assertThat(publishes).hasValue(1);
      assertThat(database.getSchema().existsType("Pv2")).isTrue();
      assertThat(database.countType("Pv2", false)).isEqualTo(2);
    }

    @Test
    void aDocumentTypeRenamePublishesExactlyOnce() {
      database.command("sql", "CREATE DOCUMENT TYPE Pd");

      final AtomicInteger publishes = countPublishes(() -> database.getSchema().getType("Pd").rename("Pd2"));

      assertThat(publishes).hasValue(1);
      assertThat(database.getSchema().existsType("Pd2")).isTrue();
    }

    /** A vertex rename refused by its nested document part leaves the type where it was, and its rename lock released. */
    @Test
    void aVertexRenameRefusedByItsDocumentPartLeavesTheTypeAndReleasesTheLock() {
      database.command("sql", "CREATE VERTEX TYPE Rb");
      database.command("sql", "CREATE VERTEX TYPE Taken");
      database.transaction(() -> database.newVertex("Rb").save());

      assertThatThrownBy(() -> database.getSchema().getType("Rb").rename("Taken")).isInstanceOf(AlreadyExistsException.class);

      assertThat(database.getSchema().existsType("Rb")).isTrue();
      assertThat(database.countType("Rb", false)).isEqualTo(1);
      // The lock was released: a second rename of the same type goes through
      database.getSchema().getType("Rb").rename("Rb2");
      assertThat(database.getSchema().existsType("Rb2")).isTrue();
    }

    /**
     * Runs {@code action} with the database's wrapper replaced by one that counts the publishing sessions a rename opens,
     * through the same seam the HA server installs its wrapper on.
     */
    private AtomicInteger countPublishes(final Runnable action) {
      final LocalDatabase local = (LocalDatabase) database;
      final DatabaseInternal previous = local.getWrappedDatabaseInstance();
      final AtomicInteger publishes = new AtomicInteger();
      local.setWrappedDatabaseInstance((DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
          new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
            if ("recordFileChanges".equals(method.getName()))
              publishes.incrementAndGet();
            try {
              return method.invoke(local, args);
            } catch (final InvocationTargetException e) {
              throw e.getCause();
            }
          }));
      try {
        action.run();
      } finally {
        local.setWrappedDatabaseInstance(previous);
      }
      return publishes;
    }
  }

  /**
   * Regression for #9978: drop type dependants under write lock.
   *
   * #9978: {@code dropType} refused a type that backs or feeds a materialized view or a continuous aggregate, but decided
   * it before taking the schema write lock and never again. A view registered on the type between that verdict and the
   * lock was left naming a type the drop then removed, and neither statement reported an error.
   * <p>
   * The window is made deterministic by holding the database write lock on a helper thread: the drop is let through
   * its early check and parks on the lock, and only then does the helper - still holding the lock - register the
   * dependant. Once the lock is released the drop must refuse. The mirror is pinned too: a create that validated its
   * source type before the lock must not register a dependant on a type dropped while it waited.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DropTypeDependantsUnderWriteLock extends TestHelper {
    private static final String DOC_SOURCE = "Issue9978Source";
    private static final String TS_SOURCE  = "Issue9978Readings";

    @Override
    protected void beginTest() {
      database.getSchema().createDocumentType(DOC_SOURCE);
      database.command("sql", "CREATE TIMESERIES TYPE " + TS_SOURCE + " TIMESTAMP ts TAGS (sensor STRING) FIELDS (temperature DOUBLE)");
      database.transaction(() -> {
        database.newDocument(DOC_SOURCE).set("name", "a").save();
        database.command("sql", "INSERT INTO " + TS_SOURCE + " SET ts = 1000, sensor = 'A', temperature = 22.5");
      });
    }

    @Test
    void aMaterializedViewRegisteredWhileTheDropWaitsForTheLockStopsTheDrop() throws Exception {
      final Throwable dropOutcome = dropWhileAnotherThreadRegistersUnderTheLock(DOC_SOURCE,
          () -> database.getSchema().buildMaterializedView().withName("Issue9978View").withQuery("SELECT FROM " + DOC_SOURCE).create());

      assertThat(dropOutcome).isInstanceOf(SchemaException.class);
      assertThat(dropOutcome.getMessage()).contains("source type for materialized view 'Issue9978View'");
      assertThat(database.getSchema().existsType(DOC_SOURCE)).isTrue();
      assertThat(database.getSchema().existsMaterializedView("Issue9978View")).isTrue();
    }

    @Test
    void aContinuousAggregateRegisteredWhileTheDropWaitsForTheLockStopsTheDrop() throws Exception {
      final Throwable dropOutcome = dropWhileAnotherThreadRegistersUnderTheLock(TS_SOURCE,
          () -> database.getSchema().buildContinuousAggregate().withName("Issue9978Hourly").withQuery(
              "SELECT sensor, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM " + TS_SOURCE + " GROUP BY sensor, hour")
              .create());

      assertThat(dropOutcome).isInstanceOf(SchemaException.class);
      assertThat(dropOutcome.getMessage()).contains("source type for continuous aggregate 'Issue9978Hourly'");
      assertThat(database.getSchema().existsType(TS_SOURCE)).isTrue();
      assertThat(database.getSchema().existsContinuousAggregate("Issue9978Hourly")).isTrue();
    }

    @Test
    void aMaterializedViewWhoseSourceIsDroppedWhileItWaitsForTheLockIsNotRegistered() throws Exception {
      final Throwable createOutcome = runWhileAnotherThreadHoldsTheLock(
          () -> database.getSchema().buildMaterializedView().withName("Issue9978Orphan").withQuery("SELECT FROM " + DOC_SOURCE).create(),
          () -> database.getSchema().dropType(DOC_SOURCE));

      assertThat(createOutcome).isInstanceOf(SchemaException.class);
      assertThat(createOutcome.getMessage()).contains(DOC_SOURCE).contains("does not exist");
      assertThat(database.getSchema().existsMaterializedView("Issue9978Orphan")).isFalse();
      assertThat(database.getSchema().existsType("Issue9978Orphan")).isFalse();
    }

    @Test
    void aContinuousAggregateWhoseSourceIsDroppedWhileItWaitsForTheLockIsNotRegistered() throws Exception {
      final Throwable createOutcome = runWhileAnotherThreadHoldsTheLock(
          () -> database.getSchema().buildContinuousAggregate().withName("Issue9978OrphanHourly").withQuery(
              "SELECT sensor, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM " + TS_SOURCE + " GROUP BY sensor, hour")
              .create(),
          () -> database.getSchema().dropType(TS_SOURCE));

      assertThat(createOutcome).isInstanceOf(SchemaException.class);
      assertThat(createOutcome.getMessage()).contains(TS_SOURCE).contains("does not exist");
      assertThat(database.getSchema().existsContinuousAggregate("Issue9978OrphanHourly")).isFalse();
      assertThat(database.getSchema().existsType("Issue9978OrphanHourly")).isFalse();
    }

    /** The behaviour the moved guard keeps without any race: a dependant still refuses the drop, by name and by alias. */
    @Test
    void theGuardStillRefusesWithoutARace() {
      database.getSchema().buildMaterializedView().withName("Issue9978Plain").withQuery("SELECT FROM " + DOC_SOURCE).create();

      assertThat(dropOutcome(DOC_SOURCE)).isInstanceOf(SchemaException.class).hasMessageContaining("source type");
      assertThat(dropOutcome("Issue9978Plain")).isInstanceOf(SchemaException.class).hasMessageContaining("backing type");

      database.command("sql", "ALTER TYPE " + DOC_SOURCE + " ALIASES Issue9978Alias");
      assertThat(dropOutcome("Issue9978Alias")).isInstanceOf(SchemaException.class).hasMessageContaining("source type");

      database.getSchema().dropMaterializedView("Issue9978Plain");
      database.getSchema().dropType(DOC_SOURCE);
      assertThat(database.getSchema().existsType(DOC_SOURCE)).isFalse();
    }

    /** A view defined through an alias stores the alias as its source: the type is still protected under both names. */
    @Test
    void aViewDefinedThroughAnAliasStillProtectsItsSourceType() {
      database.command("sql", "ALTER TYPE " + DOC_SOURCE + " ALIASES Issue9978ViaAlias");
      database.getSchema().buildMaterializedView().withName("Issue9978AliasView").withQuery("SELECT FROM Issue9978ViaAlias").create();

      assertThat(dropOutcome(DOC_SOURCE)).isInstanceOf(SchemaException.class).hasMessageContaining("Issue9978AliasView");
      assertThat(dropOutcome("Issue9978ViaAlias")).isInstanceOf(SchemaException.class).hasMessageContaining("Issue9978AliasView");
      assertThat(database.getSchema().existsType(DOC_SOURCE)).isTrue();

      database.getSchema().dropMaterializedView("Issue9978AliasView");
      database.getSchema().dropType(DOC_SOURCE);
      assertThat(database.getSchema().existsType(DOC_SOURCE)).isFalse();
    }

    private Throwable dropOutcome(final String typeName) {
      try {
        database.getSchema().dropType(typeName);
        return null;
      } catch (final Throwable t) {
        return t;
      }
    }

    private Throwable dropWhileAnotherThreadRegistersUnderTheLock(final String typeName, final Runnable registerDependant)
        throws Exception {
      return runWhileAnotherThreadHoldsTheLock(() -> database.getSchema().dropType(typeName), registerDependant);
    }

    /**
     * Runs {@code contender} on its own thread while a helper thread holds the database write lock, waits for the
     * contender to park on that lock - that is, to have made every decision it makes before the lock - then runs
     * {@code underTheLock} on the helper thread, still holding the lock, and releases it.
     *
     * @return what the contender threw, or null
     */
    private Throwable runWhileAnotherThreadHoldsTheLock(final Runnable contender, final Runnable underTheLock) throws Exception {
      final LocalDatabase embedded = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
      final CountDownLatch lockHeld = new CountDownLatch(1);
      final CountDownLatch contenderParked = new CountDownLatch(1);
      final AtomicReference<Throwable> holderFailure = new AtomicReference<>();
      final AtomicReference<Throwable> contenderOutcome = new AtomicReference<>();

      final Thread holder = new Thread(() -> {
        try {
          embedded.executeInWriteLock(() -> {
            lockHeld.countDown();
            assertThat(contenderParked.await(30, TimeUnit.SECONDS)).isTrue();
            underTheLock.run();
            return null;
          });
        } catch (final Throwable t) {
          holderFailure.set(t);
        }
      }, "Issue9978-lock-holder");

      final Thread contenderThread = new Thread(() -> {
        try {
          contender.run();
        } catch (final Throwable t) {
          contenderOutcome.set(t);
        }
      }, "Issue9978-contender");

      holder.start();
      try {
        assertThat(lockHeld.await(30, TimeUnit.SECONDS)).as("the helper thread took the database write lock").isTrue();
        contenderThread.start();
        awaitParkedOnTheWriteLock(contenderThread);
      } finally {
        contenderParked.countDown();
        holder.join(30_000);
        contenderThread.join(30_000);
      }

      assertThat(holder.isAlive()).isFalse();
      assertThat(contenderThread.isAlive()).isFalse();
      assertThat(holderFailure.get()).as("the dependant registered under the lock").isNull();
      return contenderOutcome.get();
    }

    /** The contender is waiting inside the write lock acquisition, so whatever it checks before the lock is behind it. */
    private static void awaitParkedOnTheWriteLock(final Thread thread) throws InterruptedException {
      for (int i = 0; i < 3_000; i++) {
        final Thread.State state = thread.getState();
        if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING)
          for (final StackTraceElement frame : thread.getStackTrace())
            if (frame.getClassName().equals(LocalDatabase.class.getName()) && frame.getMethodName().equals("executeInWriteLock"))
              return;
        assertThat(thread.isAlive()).as("the contender ended before it reached the write lock").isTrue();
        Thread.sleep(10);
      }
      throw new AssertionError("The contender never parked on the database write lock");
    }
  }
}
