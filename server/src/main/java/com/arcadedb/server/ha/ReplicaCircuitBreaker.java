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

import com.arcadedb.log.LogManager;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * Circuit breaker for replica connections to prevent cascading failures.
 * Implements the classic circuit breaker pattern with three states:
 * <ul>
 *   <li>CLOSED: Normal operation, messages sent to replica</li>
 *   <li>OPEN: Too many failures, replica temporarily excluded</li>
 *   <li>HALF_OPEN: Testing if replica has recovered</li>
 * </ul>
 * <p>
 * Thread-safe implementation using atomic operations.
 */
public class ReplicaCircuitBreaker {

  public enum State {
    CLOSED,    // Normal operation
    OPEN,      // Too many failures, blocking requests
    HALF_OPEN  // Testing recovery
  }

  private final int failureThreshold;
  private final int successThreshold;
  private final long retryTimeoutMs;
  private final String replicaName;

  private final AtomicInteger failureCount = new AtomicInteger(0);
  private final AtomicInteger successCount = new AtomicInteger(0);

  private volatile State state = State.CLOSED;
  private volatile long lastFailureTimestamp = 0;

  /**
   * Creates a circuit breaker for a replica connection.
   *
   * @param failureThreshold number of consecutive failures before opening circuit
   * @param successThreshold number of consecutive successes in HALF_OPEN to close circuit
   * @param retryTimeoutMs timeout in milliseconds before transitioning from OPEN to HALF_OPEN
   * @param replicaName name of the replica for logging
   */
  public ReplicaCircuitBreaker(final int failureThreshold, final int successThreshold,
                                final long retryTimeoutMs, final String replicaName) {
    this.failureThreshold = failureThreshold;
    this.successThreshold = successThreshold;
    this.retryTimeoutMs = retryTimeoutMs;
    this.replicaName = replicaName;
  }

  /**
   * Records a successful message send.
   * <ul>
   *   <li>In CLOSED state: resets failure count</li>
   *   <li>In HALF_OPEN state: increments success count, closes circuit after threshold</li>
   * </ul>
   */
  public void recordSuccess() {
    switch (state) {
    case CLOSED:
      failureCount.set(0);
      break;

    case HALF_OPEN:
      final int successes = successCount.incrementAndGet();
      failureCount.set(0);

      if (successes >= successThreshold) {
        transitionTo(State.CLOSED);
        successCount.set(0);
      }
      break;

    case OPEN:
      // Should not happen, but if it does, ignore
      break;
    }
  }

  /**
   * Records a failed message send.
   * <ul>
   *   <li>In CLOSED state: increments failure count, opens circuit after threshold</li>
   *   <li>In HALF_OPEN state: immediately reopens circuit</li>
   * </ul>
   */
  public void recordFailure() {
    lastFailureTimestamp = System.currentTimeMillis();

    switch (state) {
    case CLOSED:
      final int failures = failureCount.incrementAndGet();
      if (failures >= failureThreshold) {
        transitionTo(State.OPEN);
      }
      break;

    case HALF_OPEN:
      // Failure during recovery - reopen circuit
      successCount.set(0);
      transitionTo(State.OPEN);
      break;

    case OPEN:
      // Already open, increment failure count for tracking
      failureCount.incrementAndGet();
      break;
    }
  }

  /**
   * Checks if a message send should be attempted.
   * <ul>
   *   <li>CLOSED: always true</li>
   *   <li>OPEN: true if timeout has elapsed (transitions to HALF_OPEN)</li>
   *   <li>HALF_OPEN: true (allows probe)</li>
   * </ul>
   *
   * @return true if message send should be attempted
   */
  public boolean shouldAttempt() {
    switch (state) {
    case CLOSED:
      return true;

    case OPEN:
      // Check if timeout has elapsed
      final long timeSinceLastFailure = System.currentTimeMillis() - lastFailureTimestamp;
      if (timeSinceLastFailure >= retryTimeoutMs) {
        transitionTo(State.HALF_OPEN);
        successCount.set(0);
        return true;
      }
      return false;

    case HALF_OPEN:
      return true;

    default:
      return false;
    }
  }

  /**
   * Returns the current circuit breaker state.
   *
   * @return current state
   */
  public State getState() {
    return state;
  }

  /**
   * Returns the current failure count.
   *
   * @return failure count
   */
  public int getFailureCount() {
    return failureCount.get();
  }

  /**
   * Transitions to a new state with logging.
   *
   * @param newState the target state
   */
  private void transitionTo(final State newState) {
    if (state == newState) {
      return;
    }

    final State oldState = state;
    state = newState;

    LogManager.instance().log(this, Level.INFO,
        "Circuit breaker for replica '%s': %s -> %s (failures: %d)",
        replicaName, oldState, newState, failureCount.get());
  }

  /**
   * Forces circuit to HALF_OPEN state for testing.
   * Package-private for test use only.
   */
  void forceHalfOpen() {
    state = State.HALF_OPEN;
    successCount.set(0);
  }
}
