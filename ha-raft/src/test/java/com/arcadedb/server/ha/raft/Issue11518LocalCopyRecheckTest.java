/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.index.lsm.LSMTreeIndexMutable;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #11518: the #11264 and #11461 quarantines (a unique index with no header page, a dictionary a
 * power loss emptied) were decided only by the Raft startup scan. A copy found damaged by a later schema reload or a later open
 * was served and stayed electable.
 * <p>
 * The invariant: <b>whenever a running HA node reads its copy's index and dictionary files again - a replicated schema entry's
 * reload, an open after the startup scan - a damaged copy is quarantined the way the startup scan quarantines it</b>. The
 * snapshot-install half (an installed copy found damaged is rolled back) is {@link Issue11518InstalledCopyDamageRolledBackTest}.
 * Each test drives a state machine that never ran the startup scan, so only the path under test can raise the quarantine.
 */
class Issue11518LocalCopyRecheckTest {
  private static final String DB   = "db11518";
  private static final String TYPE = "Account";

  @TempDir
  Path root;

  private final AtomicBoolean       soleVoter = new AtomicBoolean(false);
  private       FakeRaftHAServer    raft;
  private       LocalDatabase       database;
  private       CapturingTestLogger logger;

  @BeforeEach
  void setUp() {
    raft = FakeRaftHAServer.detached();
    raft.on("isSoleVoter", args -> soleVoter.get());
    database = (LocalDatabase) new DatabaseFactory(root.resolve(DB).toString()).create();
  }

  @AfterEach
  void tearDown() {
    if (logger != null)
      logger.uninstall();
    GlobalConfiguration.HA_SCHEMA_INCREMENTAL_APPLY.reset();
    if (database != null && database.isOpen())
      database.drop();
  }

  // ---- the schema reload of a replicated schema entry, on the apply thread ----

  @Test
  void aSchemaEntryOnACopyWithAnEmptiedDictionaryQuarantinesIt() throws IOException {
    reopenWithAnEmptiedDictionary();
    final DatabaseBoundStateMachine sm = stateMachine();
    assertThat(sm.isDatabaseDiverged(DB)).as("nothing has rechecked the copy yet").isFalse();

    sm.applySchemaEntry(RaftLogEntryCodec.decode(schemaEntry()), 7L, false);

    assertThat(sm.quarantineCause(DB)).isEqualTo(DivergenceCause.EMPTY_DICTIONARY);
    assertThat(sm.isResyncInProgress()).as("the node leaves the ready set").isTrue();
  }

  @Test
  void aSchemaEntryTakingAFullReloadStillSeesTheEmptiedDictionary() throws IOException {
    // The full load builds a new dictionary instance over the page the open wrote: it must not read the copy as healthy
    GlobalConfiguration.HA_SCHEMA_INCREMENTAL_APPLY.setValue(false);
    reopenWithAnEmptiedDictionary();
    final DatabaseBoundStateMachine sm = stateMachine();

    sm.applySchemaEntry(RaftLogEntryCodec.decode(schemaEntry()), 7L, false);

    assertThat(sm.quarantineCause(DB)).isEqualTo(DivergenceCause.EMPTY_DICTIONARY);
  }

  @Test
  void aSchemaEntryOnACopyWithAHeaderlessUniqueIndexQuarantinesIt() throws Exception {
    GlobalConfiguration.HA_SCHEMA_INCREMENTAL_APPLY.setValue(false);
    final String compactedName = reopenWithAHeaderlessUniqueIndex();
    assertThat(database.getSchema().getEmbedded().getUnavailableUniqueIndexNames()).containsExactly(compactedName);
    final DatabaseBoundStateMachine sm = stateMachine();

    sm.applySchemaEntry(RaftLogEntryCodec.decode(schemaEntry()), 7L, false);

    assertThat(sm.quarantineCause(DB)).isEqualTo(DivergenceCause.UNAVAILABLE_UNIQUE_INDEX);
  }

  @Test
  void aSchemaEntryOnAHealthyCopyQuarantinesNothing() {
    createTypeWithAProperty();
    final DatabaseBoundStateMachine sm = stateMachine();

    sm.applySchemaEntry(RaftLogEntryCodec.decode(schemaEntry()), 7L, false);

    assertThat(sm.isDatabaseDiverged(DB)).isFalse();
    assertThat(raft.calls("handOffLeadershipToResync")).isEmpty();
  }

  @Test
  void aSoleVoterReportsTheDamageOnceInsteadOfOnEveryReload() throws IOException {
    soleVoter.set(true);
    reopenWithAnEmptiedDictionary();
    final DatabaseBoundStateMachine sm = stateMachine();
    logger = CapturingTestLogger.install();

    for (int i = 0; i < 3; i++)
      sm.applySchemaEntry(RaftLogEntryCodec.decode(schemaEntry()), 7L + i, false);

    assertThat(sm.isDatabaseDiverged(DB)).as("nothing could lift a quarantine on a sole voter").isFalse();
    assertThat(logger.formattedAt(Level.SEVERE)).filteredOn(line -> line.contains("only voter")).hasSize(1);
  }

  // ---- an open after the startup scan ----

  @Test
  void anOpenAfterTheStartupScanQuarantinesADamagedCopy() throws IOException {
    reopenWithAnEmptiedDictionary();
    final DatabaseBoundStateMachine sm = stateMachine();

    assertThat(sm.recheckOpenedDatabase(DB, database)).isTrue();

    assertThat(sm.quarantineCause(DB)).isEqualTo(DivergenceCause.EMPTY_DICTIONARY);
  }

  @Test
  void anOpenOfAHealthyCopyQuarantinesNothing() {
    createTypeWithAProperty();
    final DatabaseBoundStateMachine sm = stateMachine();

    assertThat(sm.recheckOpenedDatabase(DB, database)).isFalse();
    assertThat(sm.isDatabaseDiverged(DB)).isFalse();
  }

  @Test
  void theRaftServerRechecksNothingBeforeItIsBuilt() throws IOException {
    // A detached server has no Ratis server: the open of a database before the start is the startup scan's to judge
    reopenWithAnEmptiedDictionary();
    final DatabaseBoundStateMachine sm = stateMachine();
    raft.stateMachine(sm);

    raft.recheckOpenedDatabase(DB, database);

    assertThat(sm.isDatabaseDiverged(DB)).isFalse();
  }

  @Test
  void anOpenDuringASnapshotInstallOfTheSameDatabaseIsLeftToTheInstall() throws IOException {
    // Every install path lifts the quarantine on success, which would erase one raised by the install's own reopen; the
    // install validates the copy it reopens instead
    reopenWithAnEmptiedDictionary();
    final DatabaseBoundStateMachine sm = stateMachine();
    final AtomicReference<Boolean> duringInstall = new AtomicReference<>();

    sm.runUnderInstallGate(DB, () -> duringInstall.set(sm.recheckOpenedDatabase(DB, database)));

    assertThat(duringInstall.get()).isFalse();
    assertThat(sm.isDatabaseDiverged(DB)).isFalse();
    // Once the install is over the same open is judged
    assertThat(sm.recheckOpenedDatabase(DB, database)).isTrue();
  }

  // ---- fixtures ----

  private DatabaseBoundStateMachine stateMachine() {
    final DatabaseBoundStateMachine sm = new DatabaseBoundStateMachine(database);
    sm.setRaftHAServer(raft);
    return sm;
  }

  private void createTypeWithAProperty() {
    database.transaction(() -> database.getSchema().createDocumentType(TYPE).createProperty("acctId", Type.LONG));
  }

  private void reopenWithAnEmptiedDictionary() throws IOException {
    createTypeWithAProperty();
    database.close();

    final File[] dictionaryFiles = root.resolve(DB).toFile().listFiles((dir, name) -> name.endsWith("." + Dictionary.DICT_EXT));
    assertThat(dictionaryFiles).as("the fixture has a dictionary file").hasSize(1);
    try (final RandomAccessFile raf = new RandomAccessFile(dictionaryFiles[0], "rw")) {
      raf.setLength(0);
    }

    database = (LocalDatabase) new DatabaseFactory(root.resolve(DB).toString()).open();
    assertThat(database.getSchema().getEmbedded().isDictionaryLostForExistingTypes()).isTrue();
  }

  /** As {@code UnavailableUniqueIndexTest}: the unique index's mutable file swapped for an empty one. */
  private String reopenWithAHeaderlessUniqueIndex() throws Exception {
    database.transaction(() -> {
      database.getSchema().createVertexType(TYPE).createProperty("id", Type.INTEGER).createIndex(Schema.INDEX_TYPE.LSM_TREE, true);
      for (int i = 0; i < 5; i++)
        database.newVertex(TYPE).set("id", i).save();
    });
    final IndexInternal bucketIndex = database.getSchema().getType(TYPE).getAllIndexes(false).iterator().next()
        .getIndexesOnBuckets()[0];
    final LSMTreeIndexMutable mutable = ((LSMTreeIndex) bucketIndex).getMutableIndex();
    final String mutableName = mutable.getName();
    final File mutableFile = new File(database.getFileManager().getFile(mutable.getFileId()).getFilePath());
    final String compactedName = LSMTreeIndexAbstract.compactionBaseName(mutableName) + "_" + System.nanoTime();
    database.close();

    final File schemaFile = root.resolve(DB).resolve(LocalSchema.SCHEMA_FILE_NAME).toFile();
    final String schemaJson = Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8);
    Files.writeString(schemaFile.toPath(), schemaJson.replace(mutableName, compactedName), StandardCharsets.UTF_8);
    Files.deleteIfExists(root.resolve(DB).resolve(LocalSchema.SCHEMA_PREV_FILE_NAME));
    final File headerless = new File(mutableFile.getParentFile(), mutableFile.getName().replace(mutableName, compactedName));
    Files.delete(mutableFile.toPath());
    assertThat(headerless.createNewFile()).isTrue();

    database = (LocalDatabase) new DatabaseFactory(root.resolve(DB).toString()).open();
    return compactedName;
  }

  private ByteString schemaEntry() {
    return RaftLogEntryCodec.encodeSchemaEntry(DB, database.getSchema().getEmbedded().toJSON().toString(),
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList(), false, Collections.emptyList(), null, Collections.emptyMap());
  }

  private static class DatabaseBoundStateMachine extends ArcadeStateMachine {
    private final DatabaseInternal database;

    DatabaseBoundStateMachine(final DatabaseInternal database) {
      this.database = database;
    }

    @Override
    DatabaseInternal databaseFor(final String databaseName) {
      return database;
    }
  }
}
