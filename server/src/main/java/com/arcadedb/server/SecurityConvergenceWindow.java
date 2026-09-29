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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The state of the security-convergence readiness window (issue #7532, per join since #8414), owned by the
 * {@link ArcadeDBServer} rather than by a {@link ServerControlPlane} (issue #8446).
 * <p>
 * {@code ServerControlPlane} is not a singleton: the HTTP {@code /api/v1/ready} handler and the gRPC admin service
 * each construct their own. While the window lived in its instance fields, each readiness surface kept an
 * independent window: the gRPC one opened at the first gRPC probe, possibly long after the HTTP one, so the two
 * could disagree about readiness for up to a whole {@code arcadedb.ha.securityConvergenceReadinessTimeout}, and each
 * logged its own "once per window" give-up line. One holder per server makes every surface of one process read and
 * advance the same window. It is cleared by {@link #reset()} when the server starts, so an in-process restart opens
 * a window of its own, as it did when the handlers holding it were rebuilt by every start.
 * <p>
 * The fields are read and written by concurrent readiness probes on HTTP worker and gRPC threads, hence volatile;
 * the decisions made on them are {@link ServerControlPlane}'s. The give-up flag is an {@link AtomicBoolean} so that
 * the probes of both surfaces racing past the expiry still emit the SEVERE line exactly once.
 */
public final class SecurityConvergenceWindow {
  /**
   * When this node first found itself held by the gate, or {@code 0} while there is no such window open. Two probes
   * racing to open the window differ by the time between them, which is not a difference this gate can act on.
   */
  volatile long          openedAt        = 0L;
  /**
   * Whether the SEVERE give-up line has already been emitted for the window currently open, so the window expiring
   * does not log once per probe - nor once per readiness surface. Cleared together with the window.
   */
  final    AtomicBoolean giveUpLogged    = new AtomicBoolean(false);
  /**
   * The highest join index (or, on an unarmed node held after a snapshot install, install index) an armed reading of
   * the gate has seen, {@code -1} before any (issue #8414, #8432). Only a forward move opens a fresh window.
   */
  volatile long          joinIndex       = -1L;
  /**
   * The {@link #joinIndex} for which the "leading, so nobody can confirm" line was last emitted, {@code -1} before
   * any (issue #8465).
   */
  volatile long          leaderLoggedFor = -1L;

  /** Forgets every window and every logged decision: a server that (re)starts has not been held yet. */
  void reset() {
    openedAt = 0L;
    giveUpLogged.set(false);
    joinIndex = -1L;
    leaderLoggedFor = -1L;
  }
}
