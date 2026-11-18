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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.ConcurrentModificationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11921 review: a commit published outside the log takes the validation lock at its first version check, through
 * the database's page-version reservation hook. A database the leader has not validated any entry of yet had no hook, so
 * such a commit ran its check and its publication without the lock: an entry of another node validated in between read
 * the version before the publication and reserved the version the commit then published.
 * <p>
 * Real database, no cluster: the commit is phase 1 and phase 2 of a transaction, as the schema-commit arm of
 * {@code RaftReplicatedDatabase.commit()} runs them inside {@link ArcadeStateMachine#publishOutsideTheLog}, and the other
 * node's entry is the WAL of a transaction on the same page prepared before it.
 */
class Issue11921PublicationBeforeAnyValidationTest {
  private static final String TYPE = "Counter";

  @TempDir
  Path tempDir;

  private LocalDatabase      db;
  private ArcadeStateMachine stateMachine;
  private RID                leaderCounter;
  private RID                followerCounter;

  @BeforeEach
  void setUp() {
    db = (LocalDatabase) new DatabaseFactory(tempDir.resolve("issue11921").toString()).create();
    db.getSchema().createDocumentType(TYPE, 1); // one bucket: both documents share a page
    db.transaction(() -> {
      leaderCounter = db.newDocument(TYPE).set("name", "leader").set("value", 0L).save().getIdentity();
      followerCounter = db.newDocument(TYPE).set("name", "follower").set("value", 0L).save().getIdentity();
    });
    stateMachine = new ArcadeStateMachine();
  }

  @AfterEach
  void tearDown() {
    if (db != null && db.isOpen())
      db.drop();
  }

  @Test
  void aPublicationBeforeAnyValidationStillExcludesTheValidationOfAConflictingEntry() throws Exception {
    // Another node's entry, validated on its node against the page version current now; the leader has validated nothing
    // of this database yet
    final byte[] followerEntry = prepareIncrement(followerCounter);
    assertThat(db.getPageVersionReservations()).as("no validation installed the hook yet").isNull();

    final CountDownLatch validated = new CountDownLatch(1);
    final AtomicReference<Throwable> refusal = new AtomicReference<>();
    final Thread validation = new Thread(() -> {
      try {
        stateMachine.validateBeforeAppend(db, followerEntry, new PageVersionLedger.EntryId("follower", 1));
      } catch (final Throwable t) {
        refusal.set(t);
      } finally {
        validated.countDown();
      }
    }, "issue11921-follower-validation");

    final boolean validatedDuringPublication = stateMachine.publishOutsideTheLog(db, () -> {
      db.begin();
      final Document counter = db.lookupByRID(leaderCounter, true).asDocument();
      counter.modify().set("value", counter.getLong("value") + 1).save();
      final TransactionContext tx = db.getTransaction();
      final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
      // Checked, not yet published: the window the follower's entry must not be validated in
      validation.start();
      final boolean during = validated.await(500, TimeUnit.MILLISECONDS);
      tx.commit2ndPhase(phase1);
      return during;
    });
    validation.join(10_000);

    assertThat(validatedDuringPublication)
        .as("the follower's entry was validated between the publication's check and its publication").isFalse();
    assertThat(refusal.get()).as("the follower's entry claims the version the publication took")
        .isInstanceOf(ConcurrentModificationException.class);
    assertThat(stateMachine.reservedPageVersions(db.getName())).isZero();
  }

  /** Phase 1 of an increment of the counter, abandoned: the WAL bytes a node ships to the leader. */
  private byte[] prepareIncrement(final RID rid) {
    db.begin();
    final Document counter = db.lookupByRID(rid, true).asDocument();
    counter.modify().set("value", counter.getLong("value") + 1).save();
    final TransactionContext tx = db.getTransaction();
    final byte[] walData = tx.commit1stPhase(true).result.toByteArray();
    tx.rollback();
    return walData;
  }
}
