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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.lsm.LSMTreeIndexMutable;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for schema changes under the database write lock, one nested class per fixed issue.
 */
class SchemaWriteLockTest {
  /**
   * Regression for #7457: {@code schema.json} must be written INSIDE the write-locked region of
   * {@link LocalSchema#recordFileChanges}, so that no observer can list the database files and read a schema that
   * disagrees with them. Before the fix the save ran after the write lock had been released, which left a window in
   * which a {@code DROP TYPE} had already removed the bucket files while {@code schema.json} still named them - and a
   * backup archive whose schema names a bucket the archive does not contain is the failure mode a backup exists to
   * avoid.
   * <p>
   * The observer runs in the {@link DatabaseInternal.CALLBACK_EVENT#SCHEMA_AFTER_FILE_CHANGES} callback, which fires on
   * the DDL thread as the last step under the write lock: whatever the file set and the schema file look like there is
   * what the first thread to take the read lock after the release sees. Comparing the two there is exactly what a
   * concurrent backup would do, made deterministic - and a save moved back after the release would also be after the
   * hook, so the comparison would fail.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SchemaSaveUnderWriteLock extends TestHelper {

    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      return false;
    }

    @Test
    void schemaFileAgreesWithTheFileSetAsSoonAsTheWriteLockIsReleased() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final List<String> mismatches = new ArrayList<>();
      final AtomicInteger observations = new AtomicInteger();

      final Callable<Void> observer = () -> {
        // WHAT A CONCURRENT BACKUP DOES ONCE IT HAS THE READ LOCK: LIST THE FILES, READ schema.json OFF THE DISK
        observations.incrementAndGet();
        final Set<String> bucketFiles = new HashSet<>();
        for (final ComponentFile file : db.getFileManager().getFiles())
          if (file != null && LocalBucket.BUCKET_EXT.equals(file.getFileExtension()))
            bucketFiles.add(file.getComponentName());

        final Set<String> indexFiles = new HashSet<>();
        for (final ComponentFile file : db.getFileManager().getFiles())
          if (file != null && (LSMTreeIndexMutable.UNIQUE_INDEX_EXT.equals(file.getFileExtension())
              || LSMTreeIndexMutable.NOTUNIQUE_INDEX_EXT.equals(file.getFileExtension())))
            indexFiles.add(file.getComponentName());

        final File schemaFile = ((LocalSchema) db.getSchema()).getConfigurationFile();
        final Set<String> bucketsInSchemaFile = bucketsNamedBy(schemaFile);
        final Set<String> indexesInSchemaFile = indexesNamedBy(schemaFile);

        if (!bucketFiles.equals(bucketsInSchemaFile))
          mismatches.add("bucket files=" + bucketFiles + " schema.json=" + bucketsInSchemaFile);
        if (!indexFiles.equals(indexesInSchemaFile))
          mismatches.add("index files=" + indexFiles + " schema.json=" + indexesInSchemaFile);

        if (((LocalSchema) db.getSchema()).isDirty())
          mismatches.add("the schema is still dirty at the end of the write-locked region");
        return null;
      };

      db.registerCallback(DatabaseInternal.CALLBACK_EVENT.SCHEMA_AFTER_FILE_CHANGES, observer);
      try {
        for (int i = 0; i < 5; i++) {
          final String typeName = "Doc" + i;
          // CREATE REGISTERS THE BUCKET FILES BEFORE THE SCHEMA NAMES THEM: A FILE THE SCHEMA DOES NOT NAME
          final DocumentType type = database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(3).create();
          type.createProperty("id", Integer.class);
          // AN INDEX CREATION REGISTERS ONE FILE PER BUCKET BEFORE THE SCHEMA NAMES THEM
          final String indexName = type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id").getName();
          // DROP INDEX IS THE DDL THAT SAVES NOTHING OF ITS OWN: ITS FILES ARE GONE WHILE THE SCHEMA STILL NAMES THEM
          // UNTIL THE FRAME'S OWN SAVE - WHICH IS WHY THAT SAVE HAS TO RUN UNDER THE LOCK
          database.getSchema().dropIndex(indexName);
          // DROP TYPE REMOVES THE BUCKET FILES BEFORE THE SCHEMA FORGETS THEM: A SCHEMA NAMING FILES THAT ARE GONE
          database.getSchema().dropType(typeName);
        }
      } finally {
        db.unregisterCallback(DatabaseInternal.CALLBACK_EVENT.SCHEMA_AFTER_FILE_CHANGES, observer);
      }

      // AT LEAST A TYPE CREATION, AN INDEX CREATION, AN INDEX DROP AND A TYPE DROP PER ITERATION; THE NESTED BUCKET
      // CREATIONS DO NOT FIRE, THEIR FRAME IS NOT THE OUTERMOST ONE
      assertThat(observations.get()).as("the observer must have run once per outermost schema mutation")
          .isGreaterThanOrEqualTo(20);
      assertThat(mismatches).as("schema.json disagreed with the file set at the end of the write-locked region").isEmpty();
    }

    /**
     * The hook fires under the write lock - a reader started from inside it cannot get the read lock until the DDL
     * returns - and once per outermost frame, with the change already saved: for a drop, the schema file no longer
     * names the type and its bucket file is gone from the file set.
     */
    @Test
    void callbackFiresUnderTheWriteLockOncePerOutermostFrame() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      database.getSchema().buildDocumentType().withName("Gone").withTotalBuckets(2).create();
      assertThat(bucketsNamedBy(((LocalSchema) db.getSchema()).getConfigurationFile())).contains("Gone_0", "Gone_1");

      final AtomicInteger fired = new AtomicInteger();
      final AtomicInteger savedWhenFired = new AtomicInteger();
      final AtomicInteger readerRanDuringCallback = new AtomicInteger();
      final AtomicReference<Thread> reader = new AtomicReference<>();

      final Callable<Void> observer = () -> {
        fired.incrementAndGet();
        if (!db.getSchema().existsType("Gone") && db.getFileManager().getFileByComponentName("Gone_0") == null
            && !bucketsNamedBy(((LocalSchema) db.getSchema()).getConfigurationFile()).contains("Gone_0"))
          savedWhenFired.incrementAndGet();

        // THE WRITE LOCK IS HELD: A READER STARTED NOW STAYS BLOCKED UNTIL THE DDL RETURNS
        final Thread thread = new Thread(() -> db.executeInReadLock(() -> {
          readerRanDuringCallback.incrementAndGet();
          return null;
        }), "reader");
        reader.set(thread);
        thread.start();
        thread.join(500);
        assertThat(thread.isAlive()).as("the reader must be blocked by the write lock while the callback runs").isTrue();
        assertThat(readerRanDuringCallback.get()).isZero();
        return null;
      };

      db.registerCallback(DatabaseInternal.CALLBACK_EVENT.SCHEMA_AFTER_FILE_CHANGES, observer);
      try {
        database.getSchema().dropType("Gone");
      } finally {
        db.unregisterCallback(DatabaseInternal.CALLBACK_EVENT.SCHEMA_AFTER_FILE_CHANGES, observer);
      }

      assertThat(fired.get()).as("the drop of a type with two buckets is one outermost frame").isEqualTo(1);
      assertThat(savedWhenFired.get()).as("the schema file no longer named the dropped type when the callback fired").isEqualTo(1);

      reader.get().join(10_000);
      assertThat(reader.get().isAlive()).isFalse();
      assertThat(readerRanDuringCallback.get()).as("the reader runs once the DDL has released the write lock").isEqualTo(1);
    }

    private static Set<String> indexesNamedBy(final File schemaFile) throws Exception {
      final JSONObject schema = new JSONObject(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8));
      final Set<String> indexes = new HashSet<>();
      final JSONObject types = schema.getJSONObject("types", new JSONObject());
      for (final String typeName : types.keySet())
        indexes.addAll(types.getJSONObject(typeName).getJSONObject("indexes", new JSONObject()).keySet());
      return indexes;
    }

    private static Set<String> bucketsNamedBy(final File schemaFile) throws Exception {
      final JSONObject schema = new JSONObject(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8));
      final Set<String> buckets = new HashSet<>();
      final JSONObject types = schema.getJSONObject("types", new JSONObject());
      for (final String typeName : types.keySet())
        for (final Object bucket : types.getJSONObject(typeName).getJSONArray("buckets"))
          buckets.add(bucket.toString());
      return buckets;
    }
  }

  /**
   * Regression for #7672: {@code dropProperty} ran its hierarchy-wide index check OUTSIDE
   * {@code recordFileChanges}, i.e. outside the database write lock, while the sibling {@code renameProperty} - which
   * makes the identical call - runs it inside and documents why.
   * <p>
   * Two things follow from being outside. The check walks the {@code subTypes} list of this type and of every type
   * below it; those are plain {@code ArrayList}s that {@code linkSuperType}/{@code unlinkSuperType} structurally
   * modify under that same write lock, so an unlocked walk can land in a {@code ConcurrentModificationException}
   * surfacing as a 500 rather than a {@code SchemaException}. And a check that passes outside the lock can be
   * invalidated by a concurrent {@code CREATE INDEX}/{@code CREATE TYPE ... EXTENDS} before the drop applies, leaving
   * a subtype's index naming a property the super type no longer declares - which is exactly what the descendant walk
   * was added to prevent.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DropPropertyValidationUnderWriteLock extends TestHelper {

    private static final String PARENT = "Issue7672Parent";
    private static final String CHILD  = "Issue7672Child";

    @Override
    protected void beginTest() {
      database.getSchema().createDocumentType(PARENT).createProperty("code", Type.STRING);
      database.getSchema().createDocumentType(CHILD).addSuperType(PARENT);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, CHILD, "code");
    }

    /**
     * The observable consequence of the fix, and the only one a test can pin deterministically: with the database
     * write lock held elsewhere, {@code dropProperty} cannot reach its verdict at all. Before the fix it answered
     * immediately, which is precisely what "the check does not take the lock the mutation takes" means.
     */
    @Test
    void theIndexCheckWaitsForTheWriteLockTheMutationTakes() throws Exception {
      final LocalDatabase   embedded    = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
      final CountDownLatch  lockHeld    = new CountDownLatch(1);
      final CountDownLatch  releaseLock = new CountDownLatch(1);
      final ExecutorService threads     = Executors.newFixedThreadPool(2);

      try {
        final Future<?> lockHolder = threads.submit(() -> embedded.executeInWriteLock(() -> {
          lockHeld.countDown();
          assertThat(releaseLock.await(30, TimeUnit.SECONDS)).isTrue();
          return null;
        }));

        assertThat(lockHeld.await(30, TimeUnit.SECONDS)).as("the helper thread took the database write lock").isTrue();

        final AtomicReference<Throwable> dropOutcome = new AtomicReference<>();
        final Future<?> drop = threads.submit(() -> {
          try {
            database.getSchema().getType(PARENT).dropProperty("code");
          } catch (final Throwable t) {
            dropOutcome.set(t);
          }
        });

        // A short wait EXPECTED TO TIME OUT: no elapsed-time claim is being made, so a stalled JVM can only make this
        // more true. Before the fix the drop returned its SchemaException here without ever asking for the lock.
        assertThatThrownBy(() -> drop.get(2, TimeUnit.SECONDS))
            .as("dropProperty must not reach a verdict while the write lock its mutation needs is held elsewhere")
            .isInstanceOf(TimeoutException.class);

        releaseLock.countDown();
        lockHolder.get(30, TimeUnit.SECONDS);
        drop.get(30, TimeUnit.SECONDS);

        assertThat(dropOutcome.get())
            .as("and once it does get the lock it still refuses, naming the subtype's index")
            .isInstanceOf(SchemaException.class);
        assertThat(dropOutcome.get().getMessage()).contains("code").contains(CHILD);
      } finally {
        releaseLock.countDown();
        threads.shutdownNow();
        assertThat(threads.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
      }

      // Nothing was dropped: the refusal is still a refusal, not a half-applied change.
      assertThat(database.getSchema().getType(PARENT).existsProperty("code")).isTrue();
    }

    /**
     * The behaviour the moved checks must keep: a descendant's index on an inherited property still refuses the drop,
     * and a property nobody indexes still drops.
     */
    @Test
    void theMovedChecksStillRefuseAndStillAllow() {
      assertThatThrownBy(() -> database.getSchema().getType(PARENT).dropProperty("code"))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining(CHILD);

      database.getSchema().getType(PARENT).createProperty("unindexed", Type.STRING);
      assertThat(database.getSchema().getType(PARENT).dropProperty("unindexed")).isNotNull();
      assertThat(database.getSchema().getType(PARENT).existsProperty("unindexed")).isFalse();

      // A name the type does not declare is still a null answer, not an exception.
      assertThat(database.getSchema().getType(PARENT).dropProperty("neverExisted")).isNull();
    }
  }

  /**
   * Regression for #11040: concurrent {@code CREATE PROPERTY ... IF NOT EXISTS} on the same type must (1) never be refused, the
   * losers answer {@code created=false}, and (2) never lose the property from schema.json.
   */
  @Nested
  class ConcurrentCreatePropertyIfNotExists extends TestHelper {

    private static final int THREADS = 8;
    private static final int ROUNDS  = 60;

    @Test
    void concurrentIfNotExistsIsNeverRefusedAndTheLosersReportNotCreated() throws Exception {
      for (int round = 0; round < ROUNDS; ++round) {
        final String typeName = "Refused" + round;
        database.command("sql", "CREATE VERTEX TYPE " + typeName);

        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        final ConcurrentLinkedQueue<Boolean> created = new ConcurrentLinkedQueue<>();
        race(typeName, errors, created);

        assertThat(errors).as("no statement refused in round %d", round).isEmpty();
        assertThat(created.stream().filter(c -> c).count()).as("exactly one creator in round %d", round).isEqualTo(1);
      }
    }

    @Test
    void concurrentIfNotExistsKeepsThePropertyAcrossReopen() throws Exception {
      final List<String> lost = new ArrayList<>();
      for (int round = 0; round < ROUNDS; ++round) {
        final String typeName = "Lost" + round;
        database.command("sql", "CREATE VERTEX TYPE " + typeName);
        race(typeName, new ConcurrentLinkedQueue<>(), new ConcurrentLinkedQueue<>());
      }

      reopenDatabase();

      for (int round = 0; round < ROUNDS; ++round) {
        final String typeName = "Lost" + round;
        if (!database.getSchema().getType(typeName).existsProperty("name"))
          lost.add(typeName);
      }
      assertThat(lost).as("types that lost the property from schema.json").isEmpty();
    }

    /**
     * Deterministic form of the loss: two creations of the same property are both waiting for the schema write lock, so
     * both read the dirty generation before either marked it. The one that gets the lock creates the property inside a
     * transaction, which postpones the schema save to the commit. The other is refused, and its failure must not declare
     * the schema clean over the winner's unsaved change, or the commit finds nothing to save and the property never
     * reaches schema.json.
     */
    @Test
    void aRefusedCreationDoesNotDeclareTheWinnersUnsavedChangeSaved() throws Exception {
      database.getSchema().createVertexType("Held");
      final DocumentType type = database.getSchema().getType("Held");
      final DatabaseInternal db = (DatabaseInternal) database;

      final CountDownLatch refusedDone = new CountDownLatch(1);
      final AtomicInteger winners = new AtomicInteger();
      final AtomicInteger refusals = new AtomicInteger();
      final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
      final List<Thread> threads = new ArrayList<>();

      final Runnable creator = () -> {
        database.begin();
        try {
          type.createProperty("name", Type.STRING);
          winners.incrementAndGet();
          // THE REFUSED ONE MUST HAVE FAILED (AND RUN ITS CLEANUP) BEFORE THIS TRANSACTION COMMITS
          if (!refusedDone.await(30, TimeUnit.SECONDS))
            throw new IllegalStateException("the refused creation did not finish");
          database.commit();
        } catch (final AlreadyExistsException e) {
          refusals.incrementAndGet();
          database.rollback();
          refusedDone.countDown();
        } catch (final Throwable t) {
          errors.add(t);
          refusedDone.countDown();
        }
      };

      // BOTH THREADS BLOCK ON THE WRITE LOCK, HAVING ALREADY READ THE GENERATION
      db.executeInWriteLock(() -> {
        for (int i = 0; i < 2; ++i) {
          final Thread thread = new Thread(creator);
          threads.add(thread);
          thread.start();
        }
        Thread.sleep(500);
        return null;
      });

      for (final Thread thread : threads)
        thread.join(60_000);

      assertThat(errors).isEmpty();
      assertThat(winners.get()).isEqualTo(1);
      assertThat(refusals.get()).isEqualTo(1);

      reopenDatabase();
      assertThat(database.getSchema().getType("Held").existsProperty("name")).as("the property survives the reopen").isTrue();
    }

    private void race(final String typeName, final ConcurrentLinkedQueue<Throwable> errors,
        final ConcurrentLinkedQueue<Boolean> created) throws InterruptedException {
      final CountDownLatch start = new CountDownLatch(1);
      final List<Thread> threads = new ArrayList<>(THREADS);
      for (int i = 0; i < THREADS; ++i) {
        final Thread thread = new Thread(() -> {
          try {
            start.await();
            // INSIDE A TRANSACTION, AS THE HTTP COMMAND HANDLER RUNS IT: THE SCHEMA SAVE IS THEN POSTPONED TO THE COMMIT
            database.transaction(() -> {
              try (final ResultSet rs = database.command("sql", "CREATE PROPERTY " + typeName + ".name IF NOT EXISTS STRING")) {
                final Result row = rs.next();
                created.add((Boolean) row.getProperty("created"));
              }
            });
          } catch (final Throwable t) {
            errors.add(t);
          }
        });
        threads.add(thread);
        thread.start();
      }
      start.countDown();
      for (final Thread thread : threads)
        thread.join(60_000);
    }
  }
}
