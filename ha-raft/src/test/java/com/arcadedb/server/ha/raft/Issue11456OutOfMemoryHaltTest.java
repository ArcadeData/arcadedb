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
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #11456: a follower catching up a large backlog ran out of heap, its Raft apply thread died on the
 * {@link OutOfMemoryError} and the node stayed half alive, hung in a shutdown that could not allocate what it needed to finish.
 * The critical halt must end the JVM when the cause is a {@link VirtualMachineError}, whether or not the graceful stop returns,
 * and must leave the JVM alone for every other cause.
 */
class Issue11456OutOfMemoryHaltTest {

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
    assertThat(halts.get()).as("only a VirtualMachineError halts the JVM").isZero();
  }
}
