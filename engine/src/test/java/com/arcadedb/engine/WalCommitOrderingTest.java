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
import com.arcadedb.database.RID;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Map;

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
    final PageId[] victimPageId = new PageId[1];
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
      victimPageId[0] = victim.getPageId();
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
      // Evict the injected conflicting page even when an assertion failed: PageManager.INSTANCE is
      // JVM-global, and a foreign cache entry must not bleed into other tests sharing this JVM.
      if (victimPageId[0] != null)
        PageManager.INSTANCE.removePageFromCache(victimPageId[0]);
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
      final DocumentType type = db.getSchema().createDocumentType("Doc");
      type.createProperty("name", Type.STRING);
      type.createProperty("blob", Type.STRING).setExternal(true);

      final RID[] rid = new RID[1];
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
          .hasMessage("Cannot commit transaction because not all the modified resources were locked: [Doc_0_ext]");
    } finally {
      if (db.getTransaction().isActive())
        db.rollback();
      db.close();
      factory.close();
    }
  }


  @Test
  void fencedDatabaseRefusesTheWalAppendAndNewOperations() {
    // #5053 review: a commit that fails AFTER its WAL append leaves a durable-but-unpublished record; the
    // live state and the WAL diverge. The database is fenced: no further transaction may cross the WAL
    // point of no return (it could claim the same page versions the orphaned record targets), and every new
    // operation fails loudly until a close/reopen runs recovery.
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();

    final DatabaseInternal db = (DatabaseInternal) factory.create();
    try {
      db.getSchema().createDocumentType("Doc");
      db.transaction(() -> db.newDocument("Doc").set("v", 1).save());

      // An in-flight transaction that already passed phase 1 when the fence lands:
      db.begin();
      db.query("sql", "SELECT FROM Doc").next().getRecord().get().asDocument().modify().set("v", 2).save();
      final TransactionContext tx = (TransactionContext) db.getTransaction();
      final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
      assertThat(phase1).isNotNull();

      ((LocalDatabase) db).fenceForRecovery("test-injected post-WAL-append failure");

      final long walBytesBefore = totalWalBytes();
      // Either fence surface is correct: the operation choke point (checkDatabaseIsOpen, hit by the page
      // reads inside validateAndBumpVersions and wrapped by the commit) or the explicit pre-WAL-append
      // check (which guards callers whose reads never touch the choke point). What matters is the
      // invariant asserted below: nothing reached the WAL.
      assertThatThrownBy(() -> tx.commit2ndPhase(phase1))
          .as("a fenced database must refuse to cross the WAL point of no return")
          .isInstanceOf(TransactionException.class)
          .hasStackTraceContaining("fenced");
      assertThat(totalWalBytes())
          .as("the refused commit must leave NO record in the WAL").isEqualTo(walBytesBefore);

      // New operations are fenced too.
      assertThatThrownBy(() -> db.transaction(() -> db.newDocument("Doc").set("v", 3).save()))
          .hasMessageContaining("fenced");
    } finally {
      // close() must still complete on a fenced database (it is the recovery entry point).
      db.close();
      factory.close();
    }
  }


  @Test
  void publishFailureWithWalDisabledAbortsWithoutFencing() throws Exception {
    // #5053 review round 6: walAppended was set even when changes.result was null (useWAL=false, the
    // supported bulk-load mode) - a post-validation publish failure then FENCED the whole database while
    // promising a WAL replay that cannot happen (nothing was appended): a permanently unusable database for
    // a transaction that opted out of WAL durability. Such a failure must simply abort, as before this PR.
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();

    final DatabaseInternal db = (DatabaseInternal) factory.create();
    try {
      db.getSchema().createDocumentType("Doc");

      db.begin();
      db.getTransaction().setUseWAL(false);
      db.newDocument("Doc").set("v", 1).save();
      final TransactionContext tx = (TransactionContext) db.getTransaction();
      final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
      assertThat(phase1.result).as("useWAL=false must build no WAL buffer").isNull();

      injectPostPublishFailure(tx);
      assertThatThrownBy(() -> tx.commit2ndPhase(phase1)).isInstanceOf(TransactionException.class);

      assertThat(((LocalDatabase) db).isFencedForRecovery())
          .as("a publish failure with NO WAL record must abort, not fence: there is nothing to replay (#5053)")
          .isFalse();
      // The database keeps working.
      db.transaction(() -> db.newDocument("Doc").set("v", 2).save());
    } finally {
      db.close();
      factory.close();
    }
  }

  @Test
  void publishFailureAfterRealWalAppendFencesThroughTheFinallyTrigger() throws Exception {
    // The counterpart, and the wiring test for the finally trigger itself: the same post-publish failure
    // WITH the WAL enabled reaches the fence through a real exception (no manual fenceForRecovery call) -
    // the transaction is durable in the WAL, so the database must fence until a reopen replays it.
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();

    final DatabaseInternal db = (DatabaseInternal) factory.create();
    try {
      db.getSchema().createDocumentType("Doc");

      db.begin();
      db.newDocument("Doc").set("v", 1).save();
      final TransactionContext tx = (TransactionContext) db.getTransaction();
      final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
      assertThat(phase1.result).isNotNull();

      injectPostPublishFailure(tx);
      assertThatThrownBy(() -> tx.commit2ndPhase(phase1)).isInstanceOf(TransactionException.class);

      assertThat(((LocalDatabase) db).isFencedForRecovery())
          .as("a publish failure AFTER a real WAL append must fence: the tx is durable and only replay reconciles")
          .isTrue();
    } finally {
      db.close();
      factory.close();
    }
  }

  /**
   * Plants a bogus file id in the transaction's newPageCounters: commit2ndPhase resolves it via
   * Schema.getFileById AFTER publishPages, so the commit fails post-publish with a real exception flowing
   * through the real catch/finally - exactly the failure class the fence targets, without fault-injection
   * hooks.
   */
  private static void injectPostPublishFailure(final TransactionContext tx) throws Exception {
    final Field countersField = TransactionContext.class.getDeclaredField("newPageCounters");
    countersField.setAccessible(true);
    @SuppressWarnings("unchecked")
    final Map<Integer, Integer> counters = (Map<Integer, Integer>) countersField.get(tx);
    counters.put(9_999, 1);
  }

  @Test
  void remotelyCommittedRegimePreservesIdentitiesOnPreWalFailure() throws Exception {
    // #5064 (engine branch): when the HA layer marked the transaction remotely committed (quorum accepted
    // it) and the LOCAL apply then fails BEFORE the local WAL append, the finally must take the
    // reset()-without-rollback regime: the identities the cluster committed survive on the user-held
    // records (rollback would reset them to provisional and an application retry would insert duplicates),
    // and the database is NOT fenced (nothing local is durable; the Raft layer reconciles the pages).
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();

    final DatabaseInternal db = (DatabaseInternal) factory.create();
    final PageId[] victimPageId = new PageId[1];
    final PageId[] victim2PageId = new PageId[1];
    try {
      db.getSchema().createDocumentType("Doc");
      db.transaction(() -> db.newDocument("Doc").set("v", "committed").save());

      db.begin();
      final MutableDocument held = db.newDocument("Doc").set("v", "remote");
      held.save();
      final Object identityAtSave = held.getIdentity();
      assertThat(identityAtSave).isNotNull();

      final TransactionContext tx = (TransactionContext) db.getTransaction();
      final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
      assertThat(phase1).isNotNull();

      // Simulate the HA layer: quorum committed, local phase 2 about to run.
      tx.setRemotelyCommitted(true);

      // Inject a conflicting committed version so validateAndBumpVersions fails PRE-WAL (same toolkit as
      // failedValidationLeavesNoWalRecord).
      final MutablePage victim = phase1.modifiedPages.getFirst();
      victimPageId[0] = victim.getPageId();
      final MutablePage conflicting = new MutablePage(victim.getPageId(), (int) victim.getPhysicalSize(),
          victim.getContent().array().clone(), (int) (victim.getVersion() + 1), victim.getContentSize());
      PageManager.INSTANCE.putPageInReadCache(new CachedPage(conflicting, false));

      assertThatThrownBy(() -> tx.commit2ndPhase(phase1)).isInstanceOf(ConcurrentModificationException.class);

      assertThat(held.getIdentity())
          .as("the remotely-committed regime must preserve the cluster-committed identity (#5064)")
          .isEqualTo(identityAtSave);
      assertThat(((LocalDatabase) db).isFencedForRecovery())
          .as("no fence: nothing local is durable, the Raft layer reconciles").isFalse();

      // The flag is single-transaction-scoped: a FRESH transaction failing the same way must roll back
      // normally (#4940) - the sticky-flag hazard the review caught.
      db.begin();
      // Deliberately the SAME reused base context object - the reviewer's sticky-flag hazard: only
      // begin()/reset() clearing the flag protects this second transaction.
      final MutableDocument held2 = db.newDocument("Doc").set("v", "local-only");
      held2.save();
      final TransactionContext tx2 = (TransactionContext) db.getTransaction();
      final TransactionContext.TransactionPhase1 phase2 = tx2.commit1stPhase(true);
      final MutablePage victim2 = phase2.modifiedPages.getFirst();
      victim2PageId[0] = victim2.getPageId();
      final MutablePage conflicting2 = new MutablePage(victim2.getPageId(), (int) victim2.getPhysicalSize(),
          victim2.getContent().array().clone(), (int) (victim2.getVersion() + 1), victim2.getContentSize());
      PageManager.INSTANCE.putPageInReadCache(new CachedPage(conflicting2, false));
      assertThatThrownBy(() -> tx2.commit2ndPhase(phase2)).isInstanceOf(ConcurrentModificationException.class);
      assertThat(held2.getIdentity())
          .as("a plain local failure on the SAME thread context must still roll back identities (#4940)")
          .isNull();
    } finally {
      if (victimPageId[0] != null)
        PageManager.INSTANCE.removePageFromCache(victimPageId[0]);
      if (victim2PageId[0] != null)
        // Both poisoned pages evicted (#5075 review): PageManager.INSTANCE is process-global.
        PageManager.INSTANCE.removePageFromCache(victim2PageId[0]);
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
