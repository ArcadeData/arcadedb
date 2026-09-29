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

import java.util.List;

/**
 * What the issue #7532 security-convergence readiness gate sees right now, published by {@code GET /api/v1/cluster} as
 * {@code securityConvergence} (issue #8555). The gate is one of the five sources of a 503 from
 * {@code /api/v1/ready}, and the only one the status document did not carry: a node held by it answered 503 while the
 * document read fully green.
 *
 * @param held                  whether readiness is being held right now because of it
 * @param unconvergedDocuments  the security documents the cluster has not confirmed on this node, in the order users,
 *                              groups, API tokens; empty when converged or when the gate does not apply
 * @param armed                 {@code true} for a runtime joiner (added to a running cluster), {@code false} for a
 *                              static member held after a snapshot install (issue #8432)
 * @param sinceIndex            the join or install index the window is keyed by, {@code 0} when there is none
 * @param windowOpenedAt        when the current window opened, as epoch milliseconds, {@code 0} while none is open
 * @param gaveUp                {@code true} once the window expired unconverged: the node is READY and still
 *                              enforcing its own copies, which nothing but one SEVERE log line reported until now
 * @param skippedBecauseLeading {@code true} while the node leads: nobody can confirm a leader's documents, so it is not
 *                              held for them (issue #8465)
 * @param reason                the readiness body when {@code held}, otherwise {@code null}
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public record SecurityConvergenceStatus(boolean held, List<String> unconvergedDocuments, boolean armed, long sinceIndex,
                                        long windowOpenedAt, boolean gaveUp, boolean skippedBecauseLeading, String reason) {

  /** The gate does not apply, or has nothing to wait for. */
  public static final SecurityConvergenceStatus NOT_CONVERGING = new SecurityConvergenceStatus(false, List.of(), false, 0L, 0L,
      false, false, null);

  public SecurityConvergenceStatus {
    if (unconvergedDocuments == null)
      unconvergedDocuments = List.of();
  }
}
