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

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/**
 * #4936/#4937: the WAL append must be the commit's point of no return. Phase 2 used to append the
 * transaction to the WAL FIRST and validate the page versions after; a validation failure (reachable through
 * the late-joining-files hole, #4937) aborted the transaction while its record stayed in the WAL with no
 * abort marker - crash recovery then replayed the aborted transaction's other pages: partial application,
 * torn records, index entries pointing at data never committed.
 */
class WalCommitOrderingTest {

  private static final String DB_PATH = "target/databases/WalCommitOrderingTest";

  @AfterEach
  void cleanup() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    try {
      if (factory.exists())
        factory.open().drop();
    } catch (final Exception e) {
      // ignore: removed on disk below
    }
    factory.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }


  @Test
  void failedValidationLeavesNoWalRecord() throws Exception {
    // #4936: phase 2 used to append the transaction to the WAL FIRST and validate the page versions after.
    // A validation failure (reachable through the late-joining-files hole, #4937) then aborted the
    // transaction while its record stayed in the WAL with no abort marker - crash recovery replayed the
    // aborted transaction's other pages: partial application, torn records. The WAL append must be the
    // point of no return: a failed validation leaves ZERO bytes in the WAL.
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();

    final DatabaseInternal db = (DatabaseInternal) factory.create();
    try {
      db.getSchema().createDocumentType("Doc");
      db.transaction(() -> db.newDocument("Doc").set("v", "committed").save());

      // Phase 1 of a new update: locks taken, WAL buffer built, versions checked.
      db.begin();
      db.query("sql", "SELECT FROM Doc").next().getRecord().get().asDocument().modify().set("v", "doomed").save();
      final TransactionContext tx = (TransactionContext) db.getTransaction();
      final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
      assertThat(phase1).isNotNull();

      // Inject a conflicting committed version for one of the transaction's pages, as a racing transaction
      // through the (now closed) lock-coverage hole would have: phase-2 validation MUST fail.
      final MutablePage victim = phase1.modifiedPages.getFirst();
      final MutablePage conflicting = new MutablePage(victim.getPageId(), (int) victim.getPhysicalSize(),
          victim.getContent().array().clone(), (int) (victim.getVersion() + 1), victim.getContentSize());
      // putPageInReadCache is package-private (this test is in the same package): a conflicting cached
      // version makes the phase-2 version check (getMostRecentVersionOfPage) fail deterministically, exactly
      // as a racing committer through the closed lock-coverage hole would have.
      PageManager.INSTANCE.putPageInReadCache(new CachedPage(conflicting, false));

      final long walBytesBefore = totalWalBytes();
      try {
        tx.commit2ndPhase(phase1);
        fail("phase 2 must fail validation against the conflicting version");
      } catch (final ConcurrentModificationException e) {
        // expected
      }

      assertThat(totalWalBytes())
          .as("a transaction that failed validation must leave NO record in the WAL (#4936)")
          .isEqualTo(walBytesBefore);
    } finally {
      db.close();
      factory.close();
    }
  }

  @Test
  void explicitLockTransactionWithLateJoiningFileIsRejectedNotSilentlyRelocked() {
    // #4937 review: an explicit-lock transaction that touches a file it did NOT lock must fail with the
    // contract violation, not have its lock set silently expanded. An EXTERNAL property's paired bucket is
    // a genuine LATE joiner: an explicit type-lock does not cover it (collectTypeFileIds omits external
    // buckets), yet updating the property writes it during phase-1 serialization (updateRecordNoLock, AFTER
    // the initial explicit-lock check) - so only the late-joiner block added by #4937 can catch it.
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();

    final DatabaseInternal db = (DatabaseInternal) factory.create();
    try {
      final com.arcadedb.schema.DocumentType type = db.getSchema().createDocumentType("Doc");
      type.createProperty("name", com.arcadedb.schema.Type.STRING);
      type.createProperty("blob", com.arcadedb.schema.Type.STRING).setExternal(true);

      final com.arcadedb.database.RID[] rid = new com.arcadedb.database.RID[1];
      db.transaction(() -> rid[0] = db.newDocument("Doc").set("name", "a")
          .set("blob", "the quick brown fox jumps over the lazy dog").save().getIdentity());

      db.begin();
      // Lock ONLY the Doc type (primary + index buckets); the EXTERNAL 'blob' bucket is NOT covered.
      db.getTransaction().lock().type("Doc").lock();

      // Update the external property: the write to its paired bucket joins the page set late.
      db.lookupByRID(rid[0], true).asDocument().modify().set("blob", "a different, also externally-stored value").save();

      assertThatThrownBy(db::commit)
          .as("an explicit-lock tx touching an unlocked late-joining external bucket must be rejected (#4937 review)")
          .isInstanceOf(TransactionException.class)
          .hasRootCauseMessage("Cannot commit transaction because not all the modified resources were locked: [Doc_0_ext]");
    } finally {
      if (db.getTransaction().isActive())
        db.rollback();
      db.close();
      factory.close();
    }
  }

  private static long totalWalBytes() {
    long total = 0;
    final File[] walFiles = new File(DB_PATH).listFiles((d, n) -> n.endsWith(".wal"));
    if (walFiles != null)
      for (final File f : walFiles)
        total += f.length();
    return total;
  }
}
