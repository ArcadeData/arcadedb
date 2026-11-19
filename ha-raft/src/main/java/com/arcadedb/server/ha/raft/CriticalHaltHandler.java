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

import com.arcadedb.log.LogManager;
import com.arcadedb.server.ha.raft.ArcadeStateMachine.CriticalHalt;
import java.util.function.IntConsumer;
import java.util.logging.Level;

/**
 * Stops this node when an entry fails in a way the state machine cannot recover from: records the halt, stops the server
 * and, if it does not stop within the grace period, halts the JVM, so a node that could no longer apply the log never
 * keeps serving.
 * <p>
 * Split out of {@link ArcadeStateMachine}, which decides when a failure is critical.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class CriticalHaltHandler {
  private final ArcadeStateMachine sm;

  /** Exit status of the JVM halt that follows an OutOfMemoryError or InternalError in the apply path (issue #11456). */
  private static final int FATAL_ERROR_EXIT_STATUS = 1;

  /** How long the emergency stop gets to finish after an OutOfMemoryError or InternalError before the JVM is halted (issue #11456). */
  private volatile long        fatalErrorGraceMillis = 30_000L;

  private volatile Runnable    emergencyStop;

  private volatile IntConsumer jvmHalter             = status -> Runtime.getRuntime().halt(status);

  CriticalHaltHandler(final ArcadeStateMachine sm) {
    this.sm = sm;
    this.emergencyStop = () -> {
      if (sm.server != null)
        sm.server.stop();
    };
  }

  /**
   * Trips the critical-error halt and asynchronously stops the server so the node recovers via a
   * snapshot/log replay on the next start. Used by {@link ArcadeStateMachine#applyTransaction} for both unexpected
   * apply errors and unknown (un-decodable) committed entry types (issue #4798).
   * <p>
   * The halt flag is set BEFORE the async {@code server.stop()} starts so the StateMachineUpdater's
   * next {@code applyTransaction} call short-circuits instead of cascading on inconsistent state.
   * Callers must NOT advance or persist {@link ArcadeStateMachine#lastAppliedIndex} before invoking this: leaving the
   * index untouched is what lets the offending entry be replayed (instead of silently skipped) once
   * the node restarts on a compatible version.
   * <p>
   * The cause is recorded with the flag (issue #7872), because the emergency stop below can fail - its only
   * failure handling is a log line - and a process that stays up with a dead state machine has to be able to say
   * so through {@code GET /api/v1/cluster} rather than only through a SEVERE that has already scrolled past.
   * {@code compareAndSet} keeps the FIRST cause: it is the one that explains the halt, and every apply after it
   * is refused by the guard in {@code applyTransaction} rather than being a new problem.
   * <p>
   * When the cause is an {@link OutOfMemoryError} or an {@link InternalError} (issue #11456; a {@link StackOverflowError} is
   * recoverable and does not qualify) the JVM is
   * halted once the stop returns or {@link #fatalErrorGraceMillis} has passed, whichever comes first. A graceful stop
   * on an exhausted heap can hang (the Ratis service closes against a heap it cannot allocate from), and the node
   * would stay half alive: HTTP up, state machine dead, nothing for an orchestrator to restart. The Raft log and the
   * WAL are what recovery reads, so a halt loses nothing a crash would not.
   *
   * @param index  the Raft log index being applied, or -1 when there is none
   * @param reason one line naming the condition, in the vocabulary an operator can act on
   * @param cause  the error that tripped the halt, or {@code null} when it is a condition rather than an exception
   */
  void triggerCriticalHalt(final long index, final String reason, final Throwable cause) {
    sm.haltedAfterCriticalError.compareAndSet(null, new CriticalHalt(index, reason, System.currentTimeMillis()));
    final boolean fatal = isUnrecoverableJvmError(cause);
    try {
      final Thread stopThread = new Thread(() -> {
        try {
          emergencyStop.run();
        } catch (final Throwable t) {
          LogManager.instance().log(sm, Level.SEVERE, "Emergency stop failed", t);
        }
      }, "arcadedb-emergency-stop");
      stopThread.setDaemon(true);
      stopThread.start();
      if (fatal) {
        final Thread watchdog = new Thread(() -> {
          try {
            stopThread.join(fatalErrorGraceMillis);
          } catch (final InterruptedException ignored) { // noopengrep: interrupted-exception-swallowed - the JVM halts next
            // halt anyway: the JVM is unstable
          }
          haltJvm(stopThread.isAlive());
        }, "arcadedb-fatal-error-halt");
        watchdog.setDaemon(true);
        watchdog.start();
      }
    } catch (final Throwable t) {
      // No thread could be started, which on an exhausted heap is the likeliest reason: nothing graceful is left
      if (fatal)
        haltJvm(true);
    }
  }

  private void haltJvm(final boolean stopDidNotFinish) {
    try {
      LogManager.instance().log(sm, Level.SEVERE,
          "Halting the JVM after a fatal error in the Raft state machine (%s); the node restarts and recovers from its Raft log "
              + "(issue #11456)", stopDidNotFinish ? "the emergency stop did not finish in time" : "the emergency stop finished");
    } catch (final Throwable ignored) {
      // logging needs heap too
    }
    jvmHalter.accept(FATAL_ERROR_EXIT_STATUS);
  }

  private static boolean isUnrecoverableJvmError(final Throwable cause) {
    Throwable t = cause;
    for (int depth = 0; t != null && depth < 8; depth++, t = t.getCause())
      if (t instanceof OutOfMemoryError || t instanceof InternalError)
        return true;
    return false;
  }

  // @VisibleForTesting
  void setEmergencyStopForTesting(final Runnable emergencyStop) {
    this.emergencyStop = emergencyStop;
  }

  // @VisibleForTesting
  void setJvmHalterForTesting(final IntConsumer jvmHalter) {
    this.jvmHalter = jvmHalter;
  }

  // @VisibleForTesting
  void setFatalErrorGraceMillisForTesting(final long millis) {
    this.fatalErrorGraceMillis = millis;
  }
}
