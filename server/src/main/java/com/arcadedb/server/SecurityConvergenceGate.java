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
package com.arcadedb.server;

/**
 * The mutable state of the issue #7532 security-convergence readiness gate: one window per join or snapshot install,
 * owned by the server and not by a {@link ServerControlPlane}, because several of those exist at once - the HTTP
 * readiness probe, the gRPC one and the cluster status document each build their own - and a window each of them
 * opened separately would give three answers to one question (issue #8555).
 * <p>
 * Every field is volatile: they are read and written by concurrent readiness probes on HTTP worker and gRPC threads,
 * and two probes racing to open the window differ by the time between them, which is not a difference this gate can
 * act on.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class SecurityConvergenceGate {
  /**
   * When this node first found itself a member of a multi-node cluster holding none of the cluster's replicated
   * security documents, or {@code 0} while there is no such window open (issue #7532).
   */
  volatile long    windowOpenedAt   = 0L;
  /**
   * Whether the SEVERE give-up line has already been emitted for the window currently open, so the window expiring
   * does not log once per probe. Cleared together with the window, because "once" means once per window: a node that
   * converges and later opens a fresh window has a fresh decision to report.
   */
  volatile boolean giveUpLogged     = false;
  /**
   * The highest {@link HAServerPlugin#getRuntimeJoinIndex()} an armed reading of this gate has seen, {@code -1}
   * before any (issue #8414). The window is per join, not per process: when a re-add moves the join index forward the
   * window and the give-up flag are cleared exactly as convergence clears them, so the new join is held for a window of
   * its own and its give-up is reported on its own. Forward only, so a reading that reports no join index - a Raft
   * server that is not readable this tick - never restarts the bound. On an unarmed node held after a snapshot install
   * (issue #8432) it is that install's index instead: both are log positions that only move forward, and a later one of
   * either is a fresh window. One reading reports one or the other, never both: the hold is consulted only while the
   * node is unarmed, and arming is one-way.
   */
  volatile long    joinIndex        = -1L;
  /**
   * The {@link #joinIndex} for which the "leading, so nobody can confirm" line was last emitted, {@code -1} before any
   * (issue #8465): once per join or install, not once per probe.
   */
  volatile long    leaderLoggedFor  = -1L;
}
