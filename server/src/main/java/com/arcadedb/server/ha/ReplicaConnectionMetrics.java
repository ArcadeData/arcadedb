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
package com.arcadedb.server.ha;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-replica connection metrics for monitoring and diagnostics.
 */
public class ReplicaConnectionMetrics {
  // Connection health
  private final AtomicLong totalReconnections = new AtomicLong(0);
  private final AtomicLong consecutiveFailures = new AtomicLong(0);
  private final AtomicLong lastSuccessfulMessageTime = new AtomicLong(System.currentTimeMillis());
  private volatile Leader2ReplicaNetworkExecutor.STATUS currentStatus;

  // Failure categorization counts
  private final AtomicLong transientNetworkFailures = new AtomicLong(0);
  private final AtomicLong leadershipChanges = new AtomicLong(0);
  private final AtomicLong protocolErrors = new AtomicLong(0);
  private final AtomicLong unknownErrors = new AtomicLong(0);

  // Recovery performance
  private final AtomicLong totalRecoveryTimeMs = new AtomicLong(0);
  private final AtomicLong fastestRecoveryMs = new AtomicLong(Long.MAX_VALUE);
  private final AtomicLong slowestRecoveryMs = new AtomicLong(0);
  private final AtomicLong successfulRecoveries = new AtomicLong(0);
  private final AtomicLong failedRecoveries = new AtomicLong(0);

  // State transition history (last 10 transitions)
  private final ConcurrentLinkedDeque<StateTransition> recentTransitions = new ConcurrentLinkedDeque<>();

  public void recordStateChange(Leader2ReplicaNetworkExecutor.STATUS oldStatus,
                                 Leader2ReplicaNetworkExecutor.STATUS newStatus) {
    currentStatus = newStatus;

    StateTransition transition = new StateTransition(oldStatus, newStatus, System.currentTimeMillis());

    recentTransitions.addFirst(transition);
    if (recentTransitions.size() > 10) {
      recentTransitions.removeLast();
    }
  }

  public void recordSuccessfulRecovery(int attempts, long recoveryTimeMs) {
    successfulRecoveries.incrementAndGet();
    totalRecoveryTimeMs.addAndGet(recoveryTimeMs);

    fastestRecoveryMs.updateAndGet(current -> Math.min(current, recoveryTimeMs));
    slowestRecoveryMs.updateAndGet(current -> Math.max(current, recoveryTimeMs));
  }

  // Getters
  public AtomicLong getTotalReconnections() {
    return totalReconnections;
  }

  public AtomicLong getConsecutiveFailures() {
    return consecutiveFailures;
  }

  public AtomicLong getTransientNetworkFailures() {
    return transientNetworkFailures;
  }

  public AtomicLong getLeadershipChanges() {
    return leadershipChanges;
  }

  public AtomicLong getProtocolErrors() {
    return protocolErrors;
  }

  public AtomicLong getUnknownErrors() {
    return unknownErrors;
  }

  public AtomicLong getSuccessfulRecoveries() {
    return successfulRecoveries;
  }

  public AtomicLong getFailedRecoveries() {
    return failedRecoveries;
  }

  public AtomicLong getFastestRecoveryMs() {
    return fastestRecoveryMs;
  }

  public AtomicLong getSlowestRecoveryMs() {
    return slowestRecoveryMs;
  }

  public Leader2ReplicaNetworkExecutor.STATUS getCurrentStatus() {
    return currentStatus;
  }

  public ConcurrentLinkedDeque<StateTransition> getRecentTransitions() {
    return recentTransitions;
  }
}
