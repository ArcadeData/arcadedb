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
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.DuplicatedKeyException;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mockStatic;

/**
 * Issue #8635: every DDL statement rewrote {@code schema.json} several times - five for a type, two for a property
 * or an index - and each rewrite is an fsync'd atomic publication, so on a real disk a type with one property and one
 * index cost about 150 ms. Wrapping the DDL in a transaction did not help as much as it should have, because the
 * internal transactions a DDL runs (the dictionary entry for a new name, the index build) committed on their own and
 * saved the schema on the way out, outer transaction or not.
 * <p>
 * The number counted here is {@link LocalSchema#getVersion()}, which moves once per write of {@code schema.json} and
 * never otherwise, so the assertions are exact and independent of the disk the test runs on.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8635SchemaSaveOncePerDdlTest extends TestHelper {

  @Test
  void createDocumentTypeWritesTheSchemaOnce() {
    assertThat(schemaWritesOf(() -> database.getSchema().createDocumentType("Doc"))).isEqualTo(1);
    assertThat(schemaWritesOf(() -> database.getSchema().buildDocumentType().withName("Doc4").withTotalBuckets(4).create()))
        .isEqualTo(1);
  }

  @Test
  void createVertexAndEdgeTypeWriteTheSchemaOnce() {
    assertThat(schemaWritesOf(() -> database.getSchema().createVertexType("V1"))).isEqualTo(1);
    assertThat(schemaWritesOf(() -> database.getSchema().createEdgeType("E1"))).isEqualTo(1);
  }

  @Test
  void createPropertyWritesTheSchemaOnce() {
    final DocumentType type = database.getSchema().createDocumentType("Doc");
    // A NAME THE DICTIONARY HAS NEVER SEEN: ITS ENTRY IS COMMITTED BY AN INTERNAL TRANSACTION, WHICH USED TO SAVE TOO
    assertThat(schemaWritesOf(() -> type.createProperty("neverSeenBefore", Type.LONG))).isEqualTo(1);
  }

  @Test
  void createTypeIndexWritesTheSchemaOnce() {
    database.getSchema().buildDocumentType().withName("Doc").withTotalBuckets(3).create().createProperty("k", Type.LONG);
    assertThat(schemaWritesOf(
        () -> database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Doc", "k"))).isEqualTo(1);
  }

  @Test
  void createTypeIndexOnAPopulatedTypeWritesTheSchemaOnce() {
    database.getSchema().buildDocumentType().withName("Doc").withTotalBuckets(3).create().createProperty("k", Type.LONG);
    database.transaction(() -> {
      for (int i = 0; i < 300; i++)
        database.newDocument("Doc").set("k", i).save();
    });

    assertThat(schemaWritesOf(
        () -> database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Doc", "k"))).isEqualTo(1);
    assertThat(database.query("sql", "select from Doc where k = 150").stream().count()).isEqualTo(1);
  }

  @Test
  void sqlDdlStatementsWriteTheSchemaOnceEach() {
    assertThat(schemaWritesOf(() -> database.command("sql", "CREATE DOCUMENT TYPE SqlDoc"))).isEqualTo(1);
    assertThat(schemaWritesOf(() -> database.command("sql", "CREATE PROPERTY SqlDoc.k LONG"))).isEqualTo(1);
    assertThat(schemaWritesOf(() -> database.command("sql", "CREATE INDEX ON SqlDoc (k) NOTUNIQUE"))).isEqualTo(1);
    assertThat(schemaWritesOf(() -> database.command("sql", "CREATE VERTEX TYPE SqlVertex"))).isEqualTo(1);
    assertThat(schemaWritesOf(() -> database.command("sql", "CREATE EDGE TYPE SqlEdge"))).isEqualTo(1);
  }

  @Test
  void ddlScriptWritesTheSchemaOnce() {
    // A PURE-DDL SCRIPT RUNS AS ONE SCHEMA SESSION (#6990), SO THE WHOLE SCRIPT IS ONE WRITE
    assertThat(schemaWritesOf(() -> database.command("sqlscript", """
        CREATE DOCUMENT TYPE S1;
        CREATE PROPERTY S1.k LONG;
        CREATE INDEX ON S1 (k) NOTUNIQUE;
        CREATE DOCUMENT TYPE S2;
        CREATE PROPERTY S2.k LONG;
        CREATE INDEX ON S2 (k) NOTUNIQUE;
        """))).isEqualTo(1);
  }

  @Test
  void ddlInsideATransactionWritesTheSchemaOnceAtCommit() {
    final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();

    database.begin();
    final long before = schema.getVersion();
    for (int i = 0; i < 10; i++) {
      final DocumentType type = database.getSchema().createDocumentType("T" + i);
      type.createProperty("k" + i, Type.LONG);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "T" + i, "k" + i);
    }
    // NOTHING WRITTEN WHILE THE TRANSACTION IS OPEN: NOT BY THE DDL, NOT BY THE INTERNAL TRANSACTIONS IT RAN
    assertThat(schema.getVersion()).isEqualTo(before);
    assertThat(schema.isDirty()).isTrue();

    database.commit();
    assertThat(schema.getVersion()).isEqualTo(before + 1);
    assertThat(schema.isDirty()).isFalse();

    // AND THE ONE WRITE HOLDS ALL OF IT
    reopenDatabase();
    for (int i = 0; i < 10; i++) {
      assertThat(database.getSchema().existsType("T" + i)).isTrue();
      assertThat(database.getSchema().getType("T" + i).existsProperty("k" + i)).isTrue();
      assertThat(database.getSchema().getType("T" + i).getAllIndexes(false)).hasSize(1);
    }
  }

  @Test
  void bulkChangeWritesTheSchemaOnce() {
    assertThat(schemaWritesOf(() -> database.getSchema().bulkChange(() -> {
      for (int i = 0; i < 5; i++) {
        database.getSchema().createDocumentType("B" + i).createProperty("k", Type.LONG);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "B" + i, "k");
      }
    }))).isEqualTo(1);
  }

  @Test
  void ddlInsideARolledBackTransactionIsWrittenAtTheRollback() {
    final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();

    database.begin();
    final long before = schema.getVersion();
    database.getSchema().createDocumentType("Doc").createProperty("k", Type.LONG);
    assertThat(schema.getVersion()).isEqualTo(before);

    // A SCHEMA CHANGE IS NOT TRANSACTIONAL: THE TYPE AND ITS BUCKET FILE STAND AFTER THE ROLLBACK, SO THE FILE HAS TO
    // SAY SO NOW, NOT WHENEVER THE NEXT UNRELATED SCHEMA CHANGE OR THE CLOSE HAPPENS TO WRITE IT
    database.rollback();
    assertThat(schema.getVersion()).isEqualTo(before + 1);
    assertThat(schema.isDirty()).isFalse();

    reopenDatabase();
    assertThat(database.getSchema().getType("Doc").existsProperty("k")).isTrue();
  }

  @Test
  void aFailedDdlStillWritesWhatItsNestedStepsApplied() {
    // TWO BUCKETS, THE SAME KEY IN EACH: THE UNIQUE INDEX IS CREATED AND BUILT BUCKET BY BUCKET, EACH IN A TRANSACTION
    // OF ITS OWN, AND THE SECOND ONE FAILS. THE STEPS BEFORE IT LEFT THEIR SAVE TO THE DDL, WHICH NEVER GOT TO ITS OWN
    database.getSchema().buildDocumentType().withName("Doc").withTotalBuckets(2).create().createProperty("k", Type.LONG);
    database.transaction(() -> {
      database.newDocument("Doc").set("k", 1).save();
      database.newDocument("Doc").set("k", 1).save();
    });

    assertThatThrownBy(() -> database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Doc", "k"))
        .hasRootCauseInstanceOf(DuplicatedKeyException.class);

    final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();
    assertThat(schema.isDirty()).isFalse();
    final int indexesInMemory = database.getSchema().getType("Doc").getAllIndexes(false).size();

    reopenDatabase();
    assertThat(database.getSchema().getType("Doc").getAllIndexes(false)).hasSize(indexesInMemory);
    assertThat(database.countType("Doc", false)).isEqualTo(2);
  }

  /**
   * The Raft commit paths ask for the save BEFORE the ending transaction leaves the stack, where it is already
   * inactive and {@code isTransactionActive()} therefore answers {@code false} even though an enclosing transaction is
   * open. The stack depth is what tells.
   */
  @Test
  void aTransactionEndAskedBeforeTheNestedTransactionLeftTheStackDoesNotWrite() {
    final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();

    database.begin();
    final long before = schema.getVersion();
    database.getSchema().createDocumentType("Doc");
    database.begin();

    // WHAT THE RAFT READ-ONLY COMMIT DOES: RESET THE NESTED TRANSACTION, ASK FOR THE SAVE, THEN POP
    final TransactionContext nested = ((DatabaseInternal) database).getTransaction();
    nested.reset();
    assertThat(database.isTransactionActive()).as("the inactive nested transaction is still on top").isFalse();

    schema.saveConfigurationAtTransactionEnd();
    assertThat(schema.getVersion()).as("an enclosing transaction is open").isEqualTo(before);

    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).popIfNotLastTransaction();
    assertThat(database.isTransactionActive()).isTrue();
    database.commit();
    assertThat(schema.getVersion()).isEqualTo(before + 1);
  }

  @Test
  void rollbackAllNestedWritesThePostponedSchemaToo() {
    final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();

    database.begin();
    final long before = schema.getVersion();
    database.getSchema().createDocumentType("Doc");
    database.begin();
    assertThat(schema.getVersion()).isEqualTo(before);

    // THE LOOP ROLLS BACK THE LAST TRANSACTION TOO, AND LEAVES IT INACTIVE: THE POSTPONED SAVE HAPPENS THERE
    ((DatabaseInternal) database).rollbackAllNested();
    assertThat(database.isTransactionActive()).isFalse();
    assertThat(schema.getVersion()).isEqualTo(before + 1);
    assertThat(schema.isDirty()).isFalse();
  }

  /**
   * A DDL that fails inside an open transaction, after a nested step created a bucket: the save it would have made on
   * the way out is postponed by the transaction, so the schema has to stay dirty for the transaction end to write it.
   * (A duplicate key while building a unique index is no way to test this: it rolls the caller's transaction back.)
   */
  @Test
  void aFailedDdlInsideATransactionLeavesItsNestedWorkToTheTransactionEnd() {
    final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();

    database.begin();
    final long before = schema.getVersion();
    assertThatThrownBy(() -> schema.recordFileChanges(() -> {
      schema.createBucket("LeftBehind");
      throw new IllegalStateException("the DDL fails after its nested step");
    })).hasMessageContaining("the DDL fails after its nested step");

    assertThat(database.isTransactionActive()).isTrue();
    assertThat(schema.getVersion()).isEqualTo(before);
    assertThat(schema.isDirty()).as("the nested step's work is still waiting to be written").isTrue();

    database.commit();
    assertThat(schema.getVersion()).isEqualTo(before + 1);
    assertThat(schema.isDirty()).isFalse();
    assertThat(schema.existsBucket("LeftBehind")).isTrue();
  }

  /**
   * The schema is saved by LocalDatabase.commit(), not by the TransactionContext it commits: a caller that commits the
   * context directly leaves the schema dirty, and close() is the backstop that still writes it.
   */
  @Test
  void committingTheContextDirectlyLeavesTheSchemaToClose() {
    final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();

    database.begin();
    final long before = schema.getVersion();
    database.getSchema().createDocumentType("Direct");
    ((DatabaseInternal) database).getTransaction().commit();
    assertThat(schema.isDirty()).isTrue();
    assertThat(schema.getVersion()).isEqualTo(before);
    database.rollback();

    // THE ROLLBACK ABOVE HAD NOTHING LEFT TO END, BUT THE SCHEMA WAS STILL DIRTY: CLOSE WRITES IT
    reopenDatabase();
    assertThat(database.getSchema().existsType("Direct")).isTrue();
  }

  /** The write count is the schema version; the file itself has to agree that the write happened. */
  @Test
  void theSchemaFileNamesTheTypeAsSoonAsTheDdlReturns() throws Exception {
    final Path schemaFile = ((LocalSchema) database.getSchema().getEmbedded()).getConfigurationFile().toPath();
    database.getSchema().createDocumentType("OnDisk").createProperty("k", Type.LONG);
    final String content = Files.readString(schemaFile);
    assertThat(content).contains("\"OnDisk\"").contains("\"k\"");

    database.begin();
    database.getSchema().createDocumentType("Pending");
    assertThat(Files.readString(schemaFile)).as("postponed to the end of the transaction").doesNotContain("Pending");
    database.commit();
    assertThat(Files.readString(schemaFile)).contains("Pending");
  }

  /**
   * A commit that fails after the transaction ran DDL still writes it: the schema change stands whatever happened to
   * the records, and a caller that just propagates the failure never calls rollback().
   */
  @Test
  void aFailedCommitStillWritesTheDdlItsTransactionRan() {
    database.getSchema().createDocumentType("Unique").createProperty("k", Type.LONG);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Unique", "k");
    database.transaction(() -> database.newDocument("Unique").set("k", 1).save());

    final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();
    database.begin();
    final long before = schema.getVersion();
    database.getSchema().createDocumentType("CreatedInAFailedTransaction");
    database.newDocument("Unique").set("k", 1).save();
    assertThat(schema.getVersion()).isEqualTo(before);

    assertThatThrownBy(() -> database.commit()).isInstanceOf(DuplicatedKeyException.class);
    assertThat(database.isTransactionActive()).isFalse();
    assertThat(schema.getVersion()).as("written by the failed commit, no rollback() needed").isEqualTo(before + 1);
    assertThat(schema.isDirty()).isFalse();

    reopenDatabase();
    assertThat(database.getSchema().existsType("CreatedInAFailedTransaction")).isTrue();
  }

  /**
   * A transaction ending on another thread cannot write the schema while a DDL is half applied: the DDL holds the
   * database write lock, and a commit needs the read lock. What the DDL left pending is written once, by the DDL.
   */
  @Test
  void aCommitOnAnotherThreadCannotWriteTheSchemaInsideADdl() throws Exception {
    database.getSchema().createDocumentType("Busy");
    final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();
    final CountDownLatch insideTheDdl = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final CountDownLatch committed = new CountDownLatch(1);

    final Thread ddl = new Thread(() -> schema.recordFileChanges(() -> {
      schema.createBucket("HalfApplied");
      insideTheDdl.countDown();
      release.await();
      return null;
    }));
    final Thread committer = new Thread(() -> {
      database.transaction(() -> database.newDocument("Busy").set("x", 1).save());
      committed.countDown();
    });

    ddl.start();
    try {
      assertThat(insideTheDdl.await(30, TimeUnit.SECONDS)).isTrue();
      final long duringTheDdl = schema.getVersion();

      committer.start();
      // A WAIT EXPECTED TO TIME OUT: THE COMMITTER CANNOT GET PAST THE DDL'S WRITE LOCK
      assertThat(committed.await(300, TimeUnit.MILLISECONDS)).isFalse();
      assertThat(schema.getVersion()).as("nothing written while the DDL is half applied").isEqualTo(duringTheDdl);
    } finally {
      release.countDown();
      ddl.join(TimeUnit.SECONDS.toMillis(30));
    }

    assertThat(committed.await(30, TimeUnit.SECONDS)).isTrue();
    committer.join(TimeUnit.SECONDS.toMillis(30));
    assertThat(ddl.isAlive()).isFalse();
    assertThat(schema.isDirty()).isFalse();
    assertThat(database.countType("Busy", false)).isEqualTo(1);
  }

  /**
   * The deferral belongs to the thread running the DDL. A transaction ending on another thread while the DDL is in
   * flight is not the DDL's to write, and must not be left to it either.
   */
  @Test
  void aTransactionEndingOnAnotherThreadIsNotDeferredToThisThreadsDdl() throws Exception {
    final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();
    final CountDownLatch insideTheDdl = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);

    final Thread ddl = new Thread(() -> schema.recordFileChanges(() -> {
      insideTheDdl.countDown();
      release.await();
      return null;
    }));
    ddl.start();
    try {
      assertThat(insideTheDdl.await(30, TimeUnit.SECONDS)).isTrue();
      // THE FRAME MARKED THE SCHEMA DIRTY ON ENTRY
      assertThat(schema.isDirty()).isTrue();

      final long before = schema.getVersion();
      schema.saveConfigurationAtTransactionEnd();
      assertThat(schema.getVersion()).as("written by this thread, not left to the other thread's DDL").isEqualTo(before + 1);
    } finally {
      release.countDown();
      ddl.join(TimeUnit.SECONDS.toMillis(30));
    }
    assertThat(ddl.isAlive()).isFalse();
    assertThat(schema.isDirty()).isFalse();
  }

  /**
   * A DDL inside a transaction writes the schema at the end of it, so a crash before then leaves bucket and index
   * files on disk that {@code schema.json} does not name. The database has to open anyway, and the same DDL has to
   * succeed again.
   */
  @Test
  void filesOfADdlThatNeverReachedTheSchemaFileAreToleratedOnReopen() throws Exception {
    final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();
    final Path schemaFile = schema.getConfigurationFile().toPath();
    final Path previousFile = schemaFile.resolveSibling(LocalSchema.SCHEMA_PREV_FILE_NAME);
    final byte[] schemaBefore = Files.readAllBytes(schemaFile);
    final byte[] previousBefore = Files.exists(previousFile) ? Files.readAllBytes(previousFile) : null;

    database.begin();
    database.getSchema().createDocumentType("Doc").createProperty("k", Type.LONG);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Doc", "k");
    ((DatabaseInternal) database).kill();
    database.close();

    // kill() STILL SAVES THE DIRTY SCHEMA ON ITS WAY OUT: PUT BACK WHAT A CRASH BEFORE THE WRITE LEAVES
    Files.write(schemaFile, schemaBefore);
    if (previousBefore != null)
      Files.write(previousFile, previousBefore);
    else
      Files.deleteIfExists(previousFile);

    database = factory.open();
    assertThat(database.getSchema().existsType("Doc")).isFalse();

    database.getSchema().createDocumentType("Doc").createProperty("k", Type.LONG);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Doc", "k");
    database.transaction(() -> database.newDocument("Doc").set("k", 1).save());
    assertThat(database.query("sql", "select from Doc where k = 1").stream().count()).isEqualTo(1);

    reopenDatabase();
    assertThat(database.getSchema().getType("Doc").getAllIndexes(false)).hasSize(1);
    assertThat(database.query("sql", "select from Doc where k = 1").stream().count()).isEqualTo(1);
  }

  @Test
  void aSchemaWriteForcesTheDatabaseDirectoryOnce() {
    // schema.prev.json AND schema.json ARE PUBLISHED INTO THE SAME DIRECTORY, AND ONE FSYNC OF IT AFTER THE SECOND
    // RENAME MAKES BOTH DURABLE: THE COPY USED TO FORCE IT TOO, A SECOND FULL DEVICE FLUSH ON EVERY SCHEMA CHANGE
    final Path databaseDirectory = ((LocalSchema) database.getSchema().getEmbedded()).getConfigurationFile().toPath()
        .toAbsolutePath().getParent();
    final AtomicInteger opens = new AtomicInteger();
    try (final MockedStatic<FileChannel> ignored = mockStatic(FileChannel.class, invocation -> {
      if ("open".equals(invocation.getMethod().getName()) && invocation.getMethod().getParameterCount() == 2
          && databaseDirectory.equals(invocation.getArgument(0)))
        opens.incrementAndGet();
      return invocation.callRealMethod();
    })) {
      assertThat(schemaWritesOf(() -> database.getSchema().createDocumentType("Doc").createProperty("k", Type.LONG)))
          .isEqualTo(2);
    }
    assertThat(opens.get()).isEqualTo(System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win") ? 0 : 2);
  }

  @Test
  void nestedTransactionCommitDoesNotWriteWhileTheOuterOneIsOpen() {
    final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();

    database.begin();
    final long before = schema.getVersion();
    database.getSchema().createDocumentType("Doc");
    assertThat(schema.isDirty()).isTrue();

    // A PROPERTY NAME THE DICTIONARY HAS NEVER SEEN: ITS ENTRY IS COMMITTED BY A NESTED TRANSACTION, WHICH USED TO
    // WRITE THE DIRTY SCHEMA UNDER THE OUTER TRANSACTION'S FEET
    final MutableDocument doc = database.newDocument("Doc");
    doc.set("aNameTheDictionaryHasNeverSeen", 1).save();
    assertThat(schema.getVersion()).isEqualTo(before);

    database.commit();
    assertThat(schema.getVersion()).isEqualTo(before + 1);
    assertThat(schema.isDirty()).isFalse();
  }

  @Test
  void everyDdlIsDurableAfterItsSingleWrite() {
    database.getSchema().createDocumentType("Doc").createProperty("k", Type.LONG);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Doc", "k");
    database.getSchema().createVertexType("V").createProperty("name", Type.STRING);
    database.getSchema().createEdgeType("E");

    final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();
    assertThat(schema.isDirty()).isFalse();

    reopenDatabase();
    assertThat(database.getSchema().getType("Doc").existsProperty("k")).isTrue();
    assertThat(database.getSchema().getType("Doc").getAllIndexes(false)).hasSize(1);
    assertThat(database.getSchema().getType("Doc").getBuckets(false)).isNotEmpty();
    assertThat(database.getSchema().getType("V").existsProperty("name")).isTrue();
    assertThat(database.getSchema().existsType("E")).isTrue();
  }

  private long schemaWritesOf(final Runnable ddl) {
    final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();
    final long before = schema.getVersion();
    ddl.run();
    assertThat(schema.isDirty()).isFalse();
    return schema.getVersion() - before;
  }
}
