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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32C;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Per-page checksum tests (#5054, follow-up to #4926/#5052).
 * <p>
 * Async flush coalesces several committed versions into a single physical page write, so a page can go
 * v1(on-disk) -> v2 -> v3 -> v4 with only the v4 flush reaching the platter. If that flush tears (the
 * version header sector persists, the data sectors do not), the #5052 equal-version re-apply repairs only
 * v4's delta region; the regions changed by v2/v3 are skipped as strictly-older and stay silently torn.
 * The version header alone cannot tell a torn page from an intact one: a per-page CRC32C, maintained in a
 * {@code .pcrc} sidecar file on every flush, gives recovery the missing evidence, and on mismatch the FULL
 * retained WAL delta chain for that page is replayed in transaction order.
 */
class PageChecksumRecoveryTest {

  private static final String DB_PATH = "target/databases/PageChecksumRecoveryTest";

  @AfterEach
  void cleanup() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    try {
      if (factory.exists())
        factory.open().drop();
    } catch (final Exception e) {
      // A test may deliberately leave the database unreopenable: remove it on disk below.
    }
    factory.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  /**
   * The #5054 case: three committed versions coalesced into one torn flush. Doc1 changed at v2, doc2 at v3,
   * doc3 at v4 - disjoint byte regions of the SAME page. The forged tear persists the v4 version header
   * while every data sector still holds the v1 content. The old recovery skipped v2/v3 as strictly-older
   * and re-applied only v4, leaving doc1/doc2 silently stale; the checksum-driven repair must replay the
   * full chain.
   */
  @Test
  void multiVersionTornWriteIsRepairedByFullChainReplay() throws Exception {
    final String bucketFilePath = prepareTornMultiVersionScenario();

    // Forge the TORN write: the disk page still holds the v1 content, but the version header (bytes 0-3,
    // the first sector a torn flush can persist alone) claims v4. The sidecar still describes v1: a torn
    // flush that persisted the header sector but not the (earlier-issued) sidecar slot looks exactly
    // like this.
    try (final RandomAccessFile raf = new RandomAccessFile(bucketFilePath, "rw")) {
      raf.seek(0);
      assertThat(raf.readInt()).as("the updates' page must not have reached the disk").isEqualTo(1);
      raf.seek(0);
      raf.writeInt(4);
    }

    reopenAndExpectAllRepaired();
  }

  /**
   * Same tear, but the sidecar slot persisted too (slot version == disk version, CRC of the OLD bytes):
   * the equal-version CRC mismatch is the direct positive evidence of the tear.
   */
  @Test
  void tornWriteWithPersistedChecksumSlotIsRepaired() throws Exception {
    final String bucketFilePath = prepareTornMultiVersionScenario();

    try (final RandomAccessFile raf = new RandomAccessFile(bucketFilePath, "rw")) {
      raf.seek(0);
      assertThat(raf.readInt()).as("the updates' page must not have reached the disk").isEqualTo(1);
      raf.seek(0);
      raf.writeInt(4);
    }

    // Forge the sidecar slot version to match the torn header (the CRC stays the one of the v1 bytes, so
    // it can no longer match the forged page: equal version + CRC mismatch = torn page).
    final File sidecar = new File(bucketFilePath + PaginatedComponentFile.CHECKSUM_FILE_EXT);
    assertThat(sidecar).as("the checksum sidecar must have been written by the v1 flush").exists();
    try (final RandomAccessFile raf = new RandomAccessFile(sidecar, "rw")) {
      raf.seek(0);
      raf.writeInt(4);
    }

    reopenAndExpectAllRepaired();
  }

  /**
   * Migration story: a database without a sidecar (pre-checksum files, or a sidecar lost by a file copy)
   * must recover exactly as before - no verification, no repair, no failure.
   */
  @Test
  void missingSidecarSkipsVerificationAndRecoversNormally() throws Exception {
    final String bucketFilePath = prepareTornMultiVersionScenario();

    final File sidecar = new File(bucketFilePath + PaginatedComponentFile.CHECKSUM_FILE_EXT);
    if (sidecar.exists())
      assertThat(sidecar.delete()).isTrue();

    // No forged tear here: the disk page is intact at v1, so normal recovery applies v2/v3/v4 in order.
    reopenAndExpectAllRepaired();
  }

  /** Contract: every flushed page carries a sidecar slot with its version and the CRC32C of its raw bytes. */
  @Test
  void flushWritesMatchingChecksumSlot() throws Exception {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();

    final DatabaseInternal db = (DatabaseInternal) factory.create();
    try {
      db.getSchema().createDocumentType("Doc");
      db.transaction(() -> db.newDocument("Doc").set("v", "A").save());
      db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);

      final PaginatedComponent bucket = (PaginatedComponent) db.getSchema().getType("Doc").getBuckets(false).getFirst();
      final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(bucket.getFileId());

      final long slot = file.readPageChecksum(0);
      assertThat(slot).as("the flush must have written a checksum slot for page 0").isNotEqualTo(-1L);

      final ByteBuffer raw = ByteBuffer.allocate(file.getPageSize());
      file.readPage(0, raw);
      final int diskVersion = raw.getInt(0);
      final CRC32C crc = new CRC32C();
      raw.rewind();
      crc.update(raw);

      assertThat((int) (slot >>> 32)).as("slot version must match the on-disk page version").isEqualTo(diskVersion);
      assertThat((int) slot).as("slot CRC must match the CRC32C of the raw on-disk page").isEqualTo((int) crc.getValue());
    } finally {
      db.drop();
      factory.close();
    }
  }

  /** A corrupted sidecar on a cleanly-closed database (no WAL) must not affect normal opens: verification is recovery-only. */
  @Test
  void corruptedSidecarOnCleanDatabaseIsIgnoredOutsideRecovery() throws Exception {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();

    String bucketFilePath;
    {
      final DatabaseInternal db = (DatabaseInternal) factory.create();
      db.getSchema().createDocumentType("Doc");
      db.transaction(() -> db.newDocument("Doc").set("v", "A").save());
      final PaginatedComponent bucket = (PaginatedComponent) db.getSchema().getType("Doc").getBuckets(false).getFirst();
      bucketFilePath = db.getFileManager().getFile(bucket.getFileId()).getFilePath();
      db.close();
    }

    final File sidecar = new File(bucketFilePath + PaginatedComponentFile.CHECKSUM_FILE_EXT);
    assertThat(sidecar).exists();
    try (final RandomAccessFile raf = new RandomAccessFile(sidecar, "rw")) {
      raf.seek(4);
      raf.writeInt(0xDEADBEEF);
    }

    final Database reopened = factory.open();
    try {
      assertThat(reopened.query("sql", "SELECT v FROM Doc").next().<String>getProperty("v")).isEqualTo("A");
    } finally {
      reopened.close();
      factory.close();
    }
  }

  /**
   * Creates three documents in the same page (v1, flushed and durable), then commits three more
   * transactions - each updating ONE of the documents (v2, v3, v4) - with flushing suspended, and kills
   * the database. The WAL holds the v2/v3/v4 deltas; the data page on disk is still at v1.
   *
   * @return the bucket file path
   */
  private String prepareTornMultiVersionScenario() throws Exception {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();

    final String bucketFilePath;
    final DatabaseInternal db = (DatabaseInternal) factory.create();
    db.getSchema().createDocumentType("Doc");
    db.transaction(() -> {
      db.newDocument("Doc").set("id", 1, "v", "A1").save();
      db.newDocument("Doc").set("id", 2, "v", "A2").save();
      db.newDocument("Doc").set("id", 3, "v", "A3").save();
    });
    db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);

    final PaginatedComponent bucket = (PaginatedComponent) db.getSchema().getType("Doc").getBuckets(false).getFirst();
    bucketFilePath = db.getFileManager().getFile(bucket.getFileId()).getFilePath();

    assertThat(PageManager.INSTANCE.getFlushThread().setSuspended(db, true))
        .as("flush suspension must engage or the pages could reach disk, invalidating the test").isTrue();

    // Three transactions, each rewriting only its own record's byte region: v2 (doc1), v3 (doc2), v4 (doc3).
    // The new values keep the same serialized size so the update happens in place.
    for (int i = 1; i <= 3; i++) {
      final int id = i;
      db.transaction(() -> db.query("sql", "SELECT FROM Doc WHERE id = " + id).next().getRecord().get().asDocument()
          .modify().set("v", "B" + id).save());
    }

    ((LocalDatabase) db).kill();
    db.close();

    return bucketFilePath;
  }

  private void reopenAndExpectAllRepaired() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    final Database reopened = factory.open();
    try {
      final Map<Integer, String> values = new HashMap<>();
      try (final ResultSet rs = reopened.query("sql", "SELECT id, v FROM Doc")) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          values.put(r.getProperty("id"), r.getProperty("v"));
        }
      }
      assertThat(values).as("recovery must replay the FULL WAL delta chain for the torn page (#5054)")
          .containsEntry(1, "B1").containsEntry(2, "B2").containsEntry(3, "B3");
    } finally {
      reopened.close();
      factory.close();
    }
  }
}
