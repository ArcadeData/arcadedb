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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.index.lsm.LSMTreeIndexMutable;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7457: {@code schema.json} must be written INSIDE the write-locked region of
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
class Issue7457SchemaSaveUnderWriteLockTest extends TestHelper {

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
