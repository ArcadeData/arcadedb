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
package com.arcadedb.engine;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #11151: after a simulated power loss (Jepsen LazyFS) a recovered node served records whose property names resolved to
 * nothing or to another name. The schema dictionary page had several versions pending in the WAL (one per new name, each its
 * own transaction), and the power loss persisted the page's first sector - the one holding the version - while the sectors
 * holding the names appended by the earlier versions stayed stale. Recovery skipped every WAL entry older than the version on
 * disk, so only the last name's range was repaired and the dictionary kept holes where ids that records already carry should
 * have resolved.
 */
class Issue11151TornMultiVersionPageRecoveryTest {

  private static final String DB_PATH = "target/databases/Issue11151TornMultiVersionPageRecoveryTest";
  private static final String[] NAMES = { "acctId", "balance", "val", "key" };

  @BeforeEach
  void fileSyncOn() {
    GlobalConfiguration.FILE_SYNC.setValue(true);
  }

  @AfterEach
  void cleanup() {
    GlobalConfiguration.FILE_SYNC.reset();
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    try {
      if (factory.exists())
        factory.open().drop();
    } catch (final Exception e) {
      // A test may deliberately leave the database unreopenable: remove it on disk below
    }
    factory.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  private record Crashed(int[] ids, String dictionaryFilePath, long finalVersion, int finalContentSize, int firstNameOffset) {
  }

  /**
   * Adds {@link #NAMES} to the dictionary one commit each, keeps their pages off the disk and kills the database, so that the
   * WAL holds one version of dictionary page 0 per name and the data file still holds the old page.
   */
  private Crashed crashWithPendingNames(final DatabaseFactory factory) throws Exception {
    if (factory.exists())
      factory.open().drop();

    final int[] ids = new int[NAMES.length];
    final DatabaseInternal db = (DatabaseInternal) factory.create();
    db.getSchema().createDocumentType("Doc");
    db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);

    final Dictionary dictionary = db.getSchema().getDictionary();
    final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(dictionary.getFileId());

    // KEEP THE NEW NAMES OFF THE DISK: EACH ONE IS ITS OWN COMMIT, HENCE ITS OWN VERSION OF DICTIONARY PAGE 0 IN THE WAL
    assertThat(PageManager.INSTANCE.getFlushThread().setSuspended(db, true))
        .as("flush suspension must engage or the pages could reach disk, invalidating the test").isTrue();

    for (int i = 0; i < NAMES.length; ++i)
      ids[i] = dictionary.getIdByName(NAMES[i], true);

    final ImmutablePage committed = db.getPageManager()
        .getImmutablePage(new PageId(db, dictionary.getFileId(), 0), file.getPageSize(), false, false);
    // WHERE THE ENTRY OF THE FIRST NEW NAME STARTS IN THE FILE: AFTER THE PAGE HEADER, AT THE POSITION THE LOAD READS IT FROM
    int firstNameOffset = -1;
    committed.setBufferPosition(4);
    while (committed.getBufferPosition() < committed.getContentSize()) {
      final int position = committed.getBufferPosition();
      if (committed.readString().equals(NAMES[0]) && firstNameOffset < 0)
        firstNameOffset = BasePage.PAGE_HEADER_SIZE + position;
    }
    assertThat(firstNameOffset).isPositive();

    final Crashed crashed = new Crashed(ids, file.getFilePath(), committed.getVersion(), committed.getContentSize(),
        firstNameOffset);

    ((LocalDatabase) db).kill();
    db.close();
    return crashed;
  }

  /** Forges the header (sector 0) a torn flush persisted alone: the given version and the content size of the last commit. */
  private static void writeHeader(final Crashed crashed, final long version) throws Exception {
    try (final RandomAccessFile raf = new RandomAccessFile(crashed.dictionaryFilePath(), "rw")) {
      raf.seek(0);
      assertThat(raf.readInt()).as("the new names must not have reached the disk").isLessThan((int) crashed.finalVersion() - 1);
      raf.seek(0);
      raf.writeInt((int) version);
      raf.writeInt(crashed.finalContentSize());
    }
  }

  @Test
  void dictionaryTornAcrossSeveralPendingVersionsIsFullyRepairedByRecovery() throws Exception {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    final Crashed crashed = crashWithPendingNames(factory);

    // SECTOR 0 OF THE LAST VERSION REACHED THE DISK, THE NAMES BEHIND IT DID NOT
    writeHeader(crashed, crashed.finalVersion());

    final Database reopened = factory.open();
    try {
      final Dictionary dictionary = ((DatabaseInternal) reopened).getSchema().getDictionary();
      for (int i = 0; i < NAMES.length; ++i) {
        assertThat(dictionary.getIdByName(NAMES[i], false))
            .as("'%s' must keep the id records already carry (#11151)", NAMES[i]).isEqualTo(crashed.ids()[i]);
        assertThat(dictionary.getNameById(crashed.ids()[i])).isEqualTo(NAMES[i]);
      }
    } finally {
      reopened.close();
      factory.close();
    }
  }

  /**
   * A page whose disk version is beyond the newest version in the WAL had its later versions written and fsynced before their
   * WAL files were dropped. Re-applying an older entry then could overwrite bytes of those durable versions, so the entries
   * must stay skipped: a byte planted where an older entry wrote has to survive the recovery.
   */
  @Test
  void olderEntriesAreNotReappliedWhenTheWalChainToTheDiskVersionHasAGap() throws Exception {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    final Crashed crashed = crashWithPendingNames(factory);

    // THE DISK CLAIMS TWO VERSIONS BEYOND THE LAST ONE THE WAL KNOWS: NO ENTRY HAS A COMPLETE CHAIN TO IT
    writeHeader(crashed, crashed.finalVersion() + 2);

    // PLANT A SENTINEL WHERE THE ENTRY OF THE FIRST NAME WROTE: ITS LENGTH, THEN THE NAME WITH ITS FIRST LETTER CHANGED
    try (final RandomAccessFile raf = new RandomAccessFile(crashed.dictionaryFilePath(), "rw")) {
      final byte[] sentinel = ("X" + NAMES[0].substring(1)).getBytes(StandardCharsets.UTF_8);
      raf.seek(crashed.firstNameOffset());
      raf.write(sentinel.length);
      raf.write(sentinel);
    }

    final CapturingLogger captured = new CapturingLogger();
    final Logger previousLogger = LogManager.instance().getLogger();
    LogManager.instance().setLogger(captured);
    final Database reopened;
    try {
      reopened = factory.open();
    } finally {
      LogManager.instance().setLogger(previousLogger);
    }
    try {
      final Dictionary dictionary = ((DatabaseInternal) reopened).getSchema().getDictionary();
      assertThat(dictionary.getNameById(crashed.ids()[0]))
          .as("the older entry must stay skipped, the sentinel survives").isEqualTo("X" + NAMES[0].substring(1));
      // #11152: A SKIP THE REPLAY CANNOT TELL FROM A BENIGN ONE IS REPORTED, ONCE FOR THE PAGE
      assertThat(captured.warnings()).as("the skipped entries of the gapped chain are reported once per page").hasSize(1);
      assertThat(captured.warnings().get(0)).contains("cannot repair the page from the WAL");
    } finally {
      reopened.close();
      factory.close();
    }
  }

  /** The repair of a complete chain is not a skip to report: no warning. */
  @Test
  void repairedChainDoesNotWarn() throws Exception {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    final Crashed crashed = crashWithPendingNames(factory);
    writeHeader(crashed, crashed.finalVersion());

    final CapturingLogger captured = new CapturingLogger();
    final Logger previousLogger = LogManager.instance().getLogger();
    LogManager.instance().setLogger(captured);
    final Database reopened;
    try {
      reopened = factory.open();
    } finally {
      LogManager.instance().setLogger(previousLogger);
    }
    try {
      assertThat(captured.warnings()).isEmpty();
    } finally {
      reopened.close();
      factory.close();
    }
  }

  /**
   * #11152: the repair is not specific to the schema dictionary. The same replay runs for every paginated component, so a
   * bucket page (the records of a type) torn across several pending versions has to come back whole too: each record was
   * its own commit, so each one is a version of the same page.
   */
  @Test
  void bucketPageTornAcrossSeveralPendingVersionsIsFullyRepairedByRecovery() throws Exception {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();

    final int total = 5;
    final List<RID> rids = new ArrayList<>();
    final DatabaseInternal db = (DatabaseInternal) factory.create();
    db.getSchema().createDocumentType("Doc");
    db.transaction(() -> db.newDocument("Doc").set("n", -1).save());
    db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);

    final LocalBucket bucket = (LocalBucket) db.getSchema().getType("Doc").getBuckets(false).get(0);
    final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(bucket.getFileId());

    assertThat(PageManager.INSTANCE.getFlushThread().setSuspended(db, true))
        .as("flush suspension must engage or the pages could reach disk, invalidating the test").isTrue();

    for (int i = 0; i < total; ++i) {
      final int n = i;
      db.transaction(() -> {
        final MutableDocument doc = db.newDocument("Doc").set("n", n);
        doc.save();
        rids.add(doc.getIdentity());
      });
    }

    final ImmutablePage committed = db.getPageManager()
        .getImmutablePage(new PageId(db, bucket.getFileId(), 0), file.getPageSize(), false, false);
    final long finalVersion = committed.getVersion();
    final int finalContentSize = committed.getContentSize();
    final String filePath = file.getFilePath();

    ((LocalDatabase) db).kill();
    db.close();

    // SECTOR 0 OF THE LAST VERSION REACHED THE DISK, THE RECORDS BEHIND IT DID NOT
    try (final RandomAccessFile raf = new RandomAccessFile(filePath, "rw")) {
      raf.seek(0);
      assertThat(raf.readInt()).as("the new records must not have reached the disk").isLessThan((int) finalVersion - 1);
      raf.seek(0);
      raf.writeInt((int) finalVersion);
      raf.writeInt(finalContentSize);
    }

    final Database reopened = factory.open();
    try {
      for (int i = 0; i < total; ++i)
        assertThat(reopened.lookupByRID(rids.get(i), true).asDocument().getInteger("n"))
            .as("record %s must survive a torn bucket page (#11152)", rids.get(i)).isEqualTo(i);
    } finally {
      reopened.close();
      factory.close();
    }
  }

  private static final class CapturingLogger implements Logger {
    private final List<String> warnings = new CopyOnWriteArrayList<>();

    List<String> warnings() {
      return warnings;
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
        final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6,
        final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11, final Object arg12,
        final Object arg13, final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
      record(level, message);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
        final Object... args) {
      record(level, message);
    }

    @Override
    public void flush() {
      // NO-OP
    }

    private void record(final Level level, final String message) {
      if (level == Level.WARNING && message != null && message.startsWith("Crash recovery skipped the WAL entry"))
        warnings.add(message);
    }
  }
}
