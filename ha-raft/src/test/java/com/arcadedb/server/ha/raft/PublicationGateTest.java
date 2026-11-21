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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The publication gate a commit published outside the Raft log takes, and the order of the gate against validation.
 */
class PublicationGateTest {

  /**
   * Issue #11921 review: how an in-place Ratis restart acquires the publication gate before it replaces the state machine.
   * {@code restartRatis} tells an abandoned restart (shutdown or interrupt) from a failed one by reading the thread's interrupt
   * flag after the throw, so an interrupt while waiting at the gate must leave the flag set, or the restart is counted toward
   * {@code arcadedb.ha.ratisRestartMaxRetries} as a failure.
   */
  @Nested
  class PublicationGate {

    // Regression for #11921: PublicationGate
    @Test
    void anInterruptWhileWaitingForAPublicationKeepsTheInterruptFlag() throws Exception {
      assertInterruptKeepsTheFlag(60_000);
    }

    // Regression for #11921: PublicationGate
    @Test
    void anInterruptWhileWaitingWithoutABoundKeepsTheInterruptFlag() throws Exception {
      assertInterruptKeepsTheFlag(0);
    }

    // Regression for #11921: PublicationGate
    @Test
    void aPublicationOutlastingTheWaitFailsTheReplacement() throws Exception {
      final ReentrantReadWriteLock gate = new ReentrantReadWriteLock();
      final CountDownLatch publishing = new CountDownLatch(1);
      final CountDownLatch release = new CountDownLatch(1);
      final Thread publication = startPublication(gate, publishing, release);
      try {
        assertThat(publishing.await(10, TimeUnit.SECONDS)).isTrue();
        assertThatThrownBy(() -> RaftHAServer.lockPublicationGateForReplacement(gate.writeLock(), 50))
            .isInstanceOf(IOException.class).hasMessageContaining("published outside the Raft log");
        assertThat(gate.isWriteLockedByCurrentThread()).isFalse();
      } finally {
        release.countDown();
        publication.join(10_000);
      }
    }

    // Regression for #11921: PublicationGate
    @Test
    void theReplacementGetsTheGateOnceThePublicationReturns() throws Exception {
      final ReentrantReadWriteLock gate = new ReentrantReadWriteLock();
      RaftHAServer.lockPublicationGateForReplacement(gate.writeLock(), 1_000);
      try {
        assertThat(gate.isWriteLockedByCurrentThread()).isTrue();
      } finally {
        gate.writeLock().unlock();
      }
    }

    private static void assertInterruptKeepsTheFlag(final long waitMs) throws Exception {
      final ReentrantReadWriteLock gate = new ReentrantReadWriteLock();
      final CountDownLatch publishing = new CountDownLatch(1);
      final CountDownLatch release = new CountDownLatch(1);
      final Thread publication = startPublication(gate, publishing, release);
      final AtomicReference<Throwable> thrown = new AtomicReference<>();
      final AtomicBoolean interruptedAfterTheThrow = new AtomicBoolean();
      try {
        assertThat(publishing.await(10, TimeUnit.SECONDS)).isTrue();
        final Thread restart = new Thread(() -> {
          try {
            RaftHAServer.lockPublicationGateForReplacement(gate.writeLock(), waitMs);
            gate.writeLock().unlock();
          } catch (final Throwable t) {
            thrown.set(t);
            interruptedAfterTheThrow.set(Thread.currentThread().isInterrupted());
          }
        }, "issue11921-restart");
        restart.setDaemon(true);
        restart.start();

        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!gate.hasQueuedThread(restart) && System.nanoTime() < deadline)
          Thread.sleep(1);
        assertThat(gate.hasQueuedThread(restart)).as("the restart must be waiting at the gate").isTrue();

        restart.interrupt();
        restart.join(10_000);

        assertThat(thrown.get()).isInstanceOf(InterruptedException.class);
        assertThat(interruptedAfterTheThrow.get()).as("the interrupt flag restartRatis reads to tell an abandoned restart").isTrue();
      } finally {
        release.countDown();
        publication.join(10_000);
      }
    }

    private static Thread startPublication(final ReentrantReadWriteLock gate, final CountDownLatch publishing,
        final CountDownLatch release) {
      final Thread publication = new Thread(() -> {
        gate.readLock().lock();
        try {
          publishing.countDown();
          release.await(60, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          gate.readLock().unlock();
        }
      }, "issue11921-publication");
      publication.setDaemon(true);
      publication.start();
      return publication;
    }
  }

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
  @Nested
  class PublicationBeforeAnyValidation {
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

    // Regression for #11921: PublicationBeforeAnyValidation
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
}
