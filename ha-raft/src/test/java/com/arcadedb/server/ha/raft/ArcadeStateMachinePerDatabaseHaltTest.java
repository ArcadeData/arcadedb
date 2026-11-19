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

import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #4797: a single {@link ArcadeStateMachine} multiplexes every database on
 * the node, so an unexpected error while applying a committed entry for ONE database used to trip the
 * node-wide {@code haltedAfterCriticalError} flag, which then short-circuited
 * {@link ArcadeStateMachine#applyTransaction} for EVERY database until the asynchronous
 * {@code server.stop()} completed. One bad entry on one database thereby froze replication for all
 * co-located databases and forced a full process restart plus snapshot resync.
 * <p>
 * The fix scopes the failure to the affected database: the entry's apply error is reported as a
 * recoverable {@code ReplicationException} (a failed future Ratis swallows while advancing its own
 * applied index), the database is quarantined for a targeted resync, the node stays up, and entries
 * for other databases keep being applied.
 * <p>
 * The harness stands up no cluster: an empty WAL payload makes {@code applyTxEntry} fail on the first
 * thing it does - reading the entry's WAL transaction id - which is a representative "unexpected error"
 * for that database. (It used to fail one step later, on {@code server.getDatabase(...)} against the
 * {@code null} server, until issue #6965 moved the WAL read in front of it; issue #7495 is what made that
 * failure reach the per-database quarantine again, and {@code QuarantineEntryTest.TruncatedWalEntryQuarantine}
 * covers the {@code null}-server error on its own.)
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ArcadeStateMachinePerDatabaseHaltTest {

  private static TransactionContext txEntryForDatabase(final ArcadeStateMachine sm, final String databaseName,
      final long term, final long index) {
    // An empty payload is too short to hold the entry's WAL transaction id, which applyTxEntry reads
    // first, so the apply fails there without needing a server or a database (issue #7495).
    final ByteString payload = RaftLogEntryCodec.encodeTxEntry(databaseName, new byte[0], Collections.emptyMap());
    final LogEntryProto logEntry = LogEntryProto.newBuilder()
        .setTerm(term)
        .setIndex(index)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build())
        .build();
    return TransactionContext.newBuilder()
        .setStateMachine(sm)
        .setLogEntry(logEntry)
        .build();
  }

  @Test
  void perDatabaseApplyErrorDoesNotTripNodeWideHalt() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();

    final CompletableFuture<Message> future = sm.applyTransaction(txEntryForDatabase(sm, "db-A", 1L, 5L));

    // The entry fails (recoverable resync), but it must be scoped to db-A.
    assertThat(future.isCompletedExceptionally()).isTrue();
    assertThatThrownBy(future::join)
        .hasCauseInstanceOf(ReplicationException.class)
        .hasMessageContaining("per-database snapshot resync in progress");

    // The crux of #4797: the node-wide halt must NOT have been tripped, and only db-A is quarantined.
    assertThat(sm.isHaltedAfterCriticalError()).isFalse();
    assertThat(sm.isDatabaseDiverged("db-A")).isTrue();
    assertThat(sm.isDatabaseDiverged("db-B")).isFalse();
  }

  @Test
  void otherDatabasesKeepApplyingAfterOneDatabaseFails() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();

    // db-A's entry fails and quarantines db-A.
    sm.applyTransaction(txEntryForDatabase(sm, "db-A", 1L, 5L)).exceptionally(t -> null);
    assertThat(sm.isHaltedAfterCriticalError()).isFalse();

    // A subsequent entry for a DIFFERENT, healthy database must NOT be refused with the node-wide
    // "halted after critical error" message. Before the fix the global flag short-circuited it; now it
    // is processed on its own merits (and, in this null-server harness, quarantines db-B independently).
    final CompletableFuture<Message> dbB = sm.applyTransaction(txEntryForDatabase(sm, "db-B", 1L, 6L));
    assertThat(dbB.isCompletedExceptionally()).isTrue();
    assertThatThrownBy(dbB::join)
        .hasCauseInstanceOf(ReplicationException.class)
        .hasMessageContaining("Apply error on database 'db-B'");
    assertThatThrownBy(dbB::join)
        .hasMessageNotContaining("halted after critical error");

    assertThat(sm.isDatabaseDiverged("db-A")).isTrue();
    assertThat(sm.isDatabaseDiverged("db-B")).isTrue();
    assertThat(sm.isHaltedAfterCriticalError()).isFalse();
  }

  /**
   * Regression test for issue #11456: a follower catching up a large backlog ran out of heap, its Raft apply thread died on the
   * {@link OutOfMemoryError} and the node stayed half alive, hung in a shutdown that could not allocate what it needed to finish.
   * The critical halt must end the JVM when the cause is a {@link OutOfMemoryError}, whether or not the graceful stop returns,
   * and must leave the JVM alone for every other cause.
   */
  @Nested
  class OutOfMemoryHalt {

    /** A transaction whose state machine context cannot be read: the failure surfaces inside applyTransaction's guarded block. */
    private static TransactionContext failingContext(final ArcadeStateMachine sm, final Throwable failure) {
      final ByteString payload = RaftLogEntryCodec.encodeTxEntry("db", new byte[0], Collections.emptyMap());
      final LogEntryProto logEntry = LogEntryProto.newBuilder().setTerm(1L).setIndex(7L)
          .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build()).build();
      final TransactionContext real = TransactionContext.newBuilder().setStateMachine(sm).setLogEntry(logEntry).build();
      return (TransactionContext) Proxy.newProxyInstance(TransactionContext.class.getClassLoader(),
          new Class<?>[] { TransactionContext.class }, (proxy, method, args) -> {
            if (method.getName().equals("getStateMachineContext"))
              throw sneaky(failure);
            try {
              return method.invoke(real, args);
            } catch (final InvocationTargetException e) {
              throw e.getCause();
            }
          });
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> RuntimeException sneaky(final Throwable t) throws T {
      throw (T) t;
    }

    // Regression for #11456: OutOfMemoryHalt
    @Test
    void anOutOfMemoryErrorHaltsTheJvmEvenWhenTheGracefulStopHangs() throws Exception {
      final ArcadeStateMachine sm = new ArcadeStateMachine();
      final CountDownLatch neverReleased = new CountDownLatch(1);
      final CountDownLatch halted = new CountDownLatch(1);
      final AtomicInteger status = new AtomicInteger(-1);
      sm.setEmergencyStopForTesting(() -> {
        try {
          neverReleased.await();
        } catch (final InterruptedException ignored) {
          Thread.currentThread().interrupt();
        }
      });
      sm.setJvmHalterForTesting(s -> {
        status.set(s);
        halted.countDown();
      });
      sm.setFatalErrorGraceMillisForTesting(200L);

      final CompletableFuture<Message> future = sm.applyTransaction(failingContext(sm, new OutOfMemoryError("Java heap space")));

      assertThat(future.isCompletedExceptionally()).isTrue();
      assertThat(sm.isHaltedAfterCriticalError()).isTrue();
      assertThat(halted.await(30, TimeUnit.SECONDS)).as("the hung stop must not keep the half dead node up").isTrue();
      assertThat(status.get()).isNotZero();
      neverReleased.countDown();
    }

    // Regression for #11456: OutOfMemoryHalt
    @Test
    void anOutOfMemoryErrorHaltsTheJvmOnceTheStopReturns() throws Exception {
      final ArcadeStateMachine sm = new ArcadeStateMachine();
      final CountDownLatch stopped = new CountDownLatch(1);
      final CountDownLatch halted = new CountDownLatch(1);
      sm.setEmergencyStopForTesting(stopped::countDown);
      sm.setJvmHalterForTesting(s -> halted.countDown());
      sm.setFatalErrorGraceMillisForTesting(60_000L);

      sm.applyTransaction(failingContext(sm, new OutOfMemoryError("Java heap space")));

      assertThat(stopped.await(30, TimeUnit.SECONDS)).isTrue();
      assertThat(halted.await(30, TimeUnit.SECONDS)).as("a stop that returned does not leave the process up").isTrue();
    }

    // Regression for #11456: OutOfMemoryHalt
    @Test
    void anOutOfMemoryErrorWrappedInAnotherExceptionStillHalts() throws Exception {
      final ArcadeStateMachine sm = new ArcadeStateMachine();
      final CountDownLatch halted = new CountDownLatch(1);
      sm.setEmergencyStopForTesting(() -> {
      });
      sm.setJvmHalterForTesting(s -> halted.countDown());

      sm.applyTransaction(failingContext(sm, new IllegalStateException("apply failed", new OutOfMemoryError("Java heap space"))));

      assertThat(halted.await(30, TimeUnit.SECONDS)).isTrue();
    }

    // Regression for #11456: OutOfMemoryHalt
    @Test
    void aRecoverableStackOverflowDoesNotHaltTheJvm() throws Exception {
      final ArcadeStateMachine sm = new ArcadeStateMachine();
      final CountDownLatch stopped = new CountDownLatch(1);
      final AtomicInteger halts = new AtomicInteger();
      sm.setEmergencyStopForTesting(stopped::countDown);
      sm.setJvmHalterForTesting(s -> halts.incrementAndGet());
      sm.setFatalErrorGraceMillisForTesting(50L);

      sm.applyTransaction(failingContext(sm, new StackOverflowError()));

      assertThat(stopped.await(30, TimeUnit.SECONDS)).isTrue();
      Thread.sleep(500L);
      assertThat(halts.get()).as("a StackOverflowError is recoverable").isZero();
    }

    // Regression for #11456: OutOfMemoryHalt
    @Test
    void anOrdinaryCriticalErrorStopsTheServerButLeavesTheJvmAlone() throws Exception {
      final ArcadeStateMachine sm = new ArcadeStateMachine();
      final CountDownLatch stopped = new CountDownLatch(1);
      final AtomicInteger halts = new AtomicInteger();
      sm.setEmergencyStopForTesting(stopped::countDown);
      sm.setJvmHalterForTesting(s -> halts.incrementAndGet());
      sm.setFatalErrorGraceMillisForTesting(50L);

      sm.applyTransaction(failingContext(sm, new IllegalStateException("not a VM error")));

      assertThat(stopped.await(30, TimeUnit.SECONDS)).isTrue();
      assertThat(sm.isHaltedAfterCriticalError()).isTrue();
      // A negative that has to wait: well past the 50ms grace a fatal cause would have halted within
      Thread.sleep(500L);
      assertThat(halts.get()).as("only an OutOfMemoryError or InternalError halts the JVM").isZero();
    }
  }
}
