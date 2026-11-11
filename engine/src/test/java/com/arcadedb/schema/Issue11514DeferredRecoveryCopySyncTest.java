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
import com.arcadedb.engine.NodeLocalFiles;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11514: every schema write fsync'd two files, the copy of the current generation published as
 * {@code schema.prev.json} and the new {@code schema.json}, so each DDL statement issued alone paid two file flushes plus
 * the directory flush. The recovery copy is only read back when {@code schema.json} is missing, empty or unreadable, and
 * {@code schema.json} is published by an fsync'd temporary file, an atomic rename and a directory fsync, so it is never
 * torn by a crash. The copy's flush is therefore deferred: once at close (a clean close leaves it durable for the next
 * open) and, after a crash, at recovery, where a copy whose bytes did not reach the disk is rebuilt from the primary.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11514DeferredRecoveryCopySyncTest extends TestHelper {
  private final List<Path> synced = new CopyOnWriteArrayList<>();

  @Override
  protected void beginTest() {
    // The unit-test lanes turn fsync off (arcadedb.fileSync), and with it every flush this test counts
    GlobalConfiguration.FILE_SYNC.setValue(true);
    FileUtils.TEST_FILE_SYNC_HOOK = synced::add;
  }

  @Override
  protected void endTest() {
    FileUtils.TEST_FILE_SYNC_HOOK = null;
    GlobalConfiguration.FILE_SYNC.reset();
  }

  @Test
  void aDdlStatementFlushesOnlyThePrimarySchemaFile() throws Exception {
    // The first write of an open still flushes the copy: it has not yet seen the file store publish atomically
    database.getSchema().createDocumentType("Warm");

    synced.clear();
    database.getSchema().createDocumentType("Doc").createProperty("k", String.class);

    assertThat(syncsOf(LocalSchema.SCHEMA_FILE_NAME)).as("schema.json is still flushed on every write").isEqualTo(2);
    assertThat(syncsOf(LocalSchema.SCHEMA_PREV_FILE_NAME)).as("the recovery copy is not flushed per write").isZero();
    // The copy is still published on every write, and holds the generation before the current one
    assertThat(prevFile()).exists();
    assertThat(versionOf(prevFile())).isEqualTo(versionOf(schemaFile()) - 1);
  }

  @Test
  void theFirstWriteOfAnOpenFlushesTheCopyUntilAnAtomicPublicationIsSeen() {
    database.getSchema().createDocumentType("Before");
    reopenDatabase();

    synced.clear();
    database.getSchema().createDocumentType("First");
    assertThat(syncsOf(LocalSchema.SCHEMA_PREV_FILE_NAME)).as("nothing is known yet about this open's file store").isEqualTo(1);

    synced.clear();
    database.getSchema().createDocumentType("Second");
    assertThat(syncsOf(LocalSchema.SCHEMA_PREV_FILE_NAME)).isZero();
  }

  @Test
  void aCleanCloseFlushesTheRecoveryCopyOnce() throws Exception {
    database.getSchema().createDocumentType("Warm");
    database.getSchema().createDocumentType("A");
    database.getSchema().createDocumentType("B");
    final byte[] prevBeforeClose = Files.readAllBytes(prevFile());

    synced.clear();
    reopenDatabase();
    assertThat(syncsOf(LocalSchema.SCHEMA_PREV_FILE_NAME)).as("the close made the deferred copy durable").isEqualTo(1);
    assertThat(Files.readAllBytes(prevFile())).isEqualTo(prevBeforeClose);

    // Nothing written since the open: nothing owed at the next close
    synced.clear();
    reopenDatabase();
    assertThat(syncsOf(LocalSchema.SCHEMA_PREV_FILE_NAME)).isZero();
  }

  @Test
  void aRecoveryCopyLostInACrashIsRebuiltFromThePrimaryAtRecovery() throws Exception {
    database.getSchema().createDocumentType("Warm");
    database.getSchema().createDocumentType("A");
    crashAndLoseTheRecoveryCopy(new byte[0]);

    synced.clear();
    database = factory.open();

    assertThat(database.getSchema().existsType("A")).isTrue();
    assertThat(syncsOf(LocalSchema.SCHEMA_PREV_FILE_NAME)).as("rebuilt durably at recovery").isEqualTo(1);
    assertThat(Files.readString(prevFile())).isEqualTo(Files.readString(schemaFile()));
    assertThat(versionOf(prevFile())).isEqualTo(versionOf(schemaFile()));
  }

  @Test
  void aRecoveryCopyLeftWithZeroedBlocksIsRebuiltToo() throws Exception {
    database.getSchema().createDocumentType("Warm");
    database.getSchema().createDocumentType("A");
    // What a file system that allocated the blocks without writing them leaves: the right length, all zeroes
    crashAndLoseTheRecoveryCopy(new byte[(int) Files.size(prevFile())]);

    database = factory.open();

    assertThat(Files.readString(prevFile())).isEqualTo(Files.readString(schemaFile()));
  }

  @Test
  void aRecoveryCopyThatParsesButIsNoLoadableSchemaIsRebuiltToo() throws Exception {
    database.getSchema().createDocumentType("Warm");
    database.getSchema().createDocumentType("A");
    // Valid JSON with a version, but without the settings and types the load requires
    crashAndLoseTheRecoveryCopy("{\"schemaVersion\":7}".getBytes(StandardCharsets.UTF_8));

    database = factory.open();

    assertThat(Files.readString(prevFile())).isEqualTo(Files.readString(schemaFile()));
  }

  @Test
  void aReadableRecoveryCopyKeepsItsGenerationAndIsMadeDurableAtRecovery() throws Exception {
    database.getSchema().createDocumentType("Warm");
    database.getSchema().createDocumentType("A");
    final byte[] prevBeforeCrash = Files.readAllBytes(prevFile());
    crashAndLoseTheRecoveryCopy(null);

    synced.clear();
    database = factory.open();

    // A process crash leaves the copy in the page cache, where a later power failure can still drop it
    assertThat(syncsOf(LocalSchema.SCHEMA_PREV_FILE_NAME)).isEqualTo(1);
    assertThat(Files.readAllBytes(prevFile())).as("the previous generation is kept, not replaced").isEqualTo(prevBeforeCrash);
    assertThat(versionOf(prevFile())).isEqualTo(versionOf(schemaFile()) - 1);
  }

  @Test
  void theRecoveryCopyStillServesAsTheFallbackForADamagedPrimary() throws Exception {
    database.getSchema().createDocumentType("Warm");
    database.getSchema().createDocumentType("A");
    database.getSchema().createDocumentType("B");
    database.close();

    // The primary is damaged after a clean close: the copy, flushed by that close, is what the next open reads
    Files.writeString(schemaFile(), "{ not json");
    database = factory.open();

    assertThat(database.getSchema().existsType("A")).isTrue();
    assertThat(((LocalSchema) database.getSchema().getEmbedded()).isLoadedFromPreviousSchema()).isTrue();
  }

  @Test
  void aRecoveryCopyWhoseNameWasLostInACrashIsRebuiltAtRecovery() throws Exception {
    database.getSchema().createDocumentType("Warm");
    database.getSchema().createDocumentType("A");
    crashAndLoseTheRecoveryCopy(null);
    Files.delete(prevFile());

    synced.clear();
    database = factory.open();

    assertThat(syncsOf(LocalSchema.SCHEMA_PREV_FILE_NAME)).isEqualTo(1);
    assertThat(Files.readString(prevFile())).isEqualTo(Files.readString(schemaFile()));
  }

  @Test
  void aFailedFlushAtCloseKeepsTheLockFileSoTheNextOpenReSecuresTheCopy() throws Exception {
    database.getSchema().createDocumentType("Warm");
    database.getSchema().createDocumentType("A");
    final Path lockFile = Path.of(getDatabasePath()).toAbsolutePath().resolve(NodeLocalFiles.DATABASE_LOCK_FILE_NAME);

    // A copy the close cannot open for writing, whatever the privileges of the runner: a directory under its name
    final byte[] prevContent = Files.readAllBytes(prevFile());
    Files.delete(prevFile());
    Files.createDirectory(prevFile());
    try {
      database.close();
      assertThat(lockFile).as("kept as the marker that makes the next open re-secure the copy").exists();
    } finally {
      Files.delete(prevFile());
      Files.write(prevFile(), prevContent);
    }

    synced.clear();
    database = factory.open();

    assertThat(database.getSchema().existsType("A")).isTrue();
    assertThat(syncsOf(LocalSchema.SCHEMA_PREV_FILE_NAME)).as("re-secured by the recovery").isEqualTo(1);
    assertThat(((LocalSchema) database.getSchema().getEmbedded()).isPreviousSchemaCopyUnsynced()).isFalse();

    // A clean close after that deletes the marker as usual
    database.close();
    assertThat(lockFile).doesNotExist();
    database = factory.open();
  }

  @Test
  void aCopyTheRecoveryCouldNotRebuildIsRebuiltByTheClose() throws Exception {
    // Missing by the close: there is nothing to flush, the close has to rebuild it
    closeAfterARecoveryThatCouldNotRebuildTheCopy(null);
  }

  @Test
  void aDamagedCopyTheRecoveryCouldNotRebuildIsRebuiltByTheCloseNotFlushed() throws Exception {
    // Still damaged by the close: flushing it would secure a copy that cannot be loaded, the close has to rebuild it
    closeAfterARecoveryThatCouldNotRebuildTheCopy(new byte[512]);
  }

  /**
   * The recovery's rebuild of a copy lost in the crash fails (a directory under its name is in the way), then the directory
   * gives way to {@code copyAtClose} ({@code null}: nothing) before the close, which must rebuild the copy from the primary.
   */
  private void closeAfterARecoveryThatCouldNotRebuildTheCopy(final byte[] copyAtClose) throws Exception {
    database.getSchema().createDocumentType("Warm");
    database.getSchema().createDocumentType("A");
    crashAndLoseTheRecoveryCopy(null);
    final Path lockFile = Path.of(getDatabasePath()).toAbsolutePath().resolve(NodeLocalFiles.DATABASE_LOCK_FILE_NAME);

    Files.delete(prevFile());
    Files.createDirectory(prevFile());
    database = factory.open();
    assertThat(((LocalSchema) database.getSchema().getEmbedded()).isPreviousSchemaCopyUnsynced()).isTrue();

    Files.delete(prevFile());
    if (copyAtClose != null)
      Files.write(prevFile(), copyAtClose);
    database.close();

    assertThat(Files.readString(prevFile())).isEqualTo(Files.readString(schemaFile()));
    assertThat(lockFile).doesNotExist();
    database = factory.open();
  }

  /**
   * Simulates a crash after which {@code schema.prev.json} holds {@code prevContent} ({@code null}: left as it was). The kill
   * runs the schema close too, so fsync is off meanwhile: the copy reaches the recovery with its flush still owed.
   */
  private void crashAndLoseTheRecoveryCopy(final byte[] prevContent) throws Exception {
    synced.clear();
    GlobalConfiguration.FILE_SYNC.setValue(false);
    try {
      ((DatabaseInternal) database).kill();
      database.close();
    } finally {
      GlobalConfiguration.FILE_SYNC.setValue(true);
    }
    assertThat(syncsOf(LocalSchema.SCHEMA_PREV_FILE_NAME)).as("the simulated crash flushed nothing").isZero();
    if (prevContent != null)
      Files.write(prevFile(), prevContent);
  }

  private long syncsOf(final String fileName) {
    return synced.stream().filter(p -> p.getFileName().toString().equals(fileName)).count();
  }

  private Path schemaFile() {
    return Path.of(getDatabasePath()).toAbsolutePath().resolve(LocalSchema.SCHEMA_FILE_NAME);
  }

  private Path prevFile() {
    return Path.of(getDatabasePath()).toAbsolutePath().resolve(LocalSchema.SCHEMA_PREV_FILE_NAME);
  }

  private static long versionOf(final Path file) throws Exception {
    return new JSONObject(Files.readString(file)).getLong("schemaVersion");
  }
}
